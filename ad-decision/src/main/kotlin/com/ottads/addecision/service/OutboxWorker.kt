package com.ottads.addecision.service

import com.ottads.addecision.config.OutboxProperties
import com.ottads.addecision.metrics.OutboxMetrics
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.sql.Timestamp
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

data class OutboxRow(
    val id: Long,
    val eventId: String,
    val eventType: String,
    val aggregateId: String,
    val payload: String,
    val occurredAt: Timestamp,
)

/**
 * Outbox 워커.
 *
 *   1) published = false 인 행을 id 순으로 배치 조회
 *   2) Kafka ad.request 로 발행 (동기, 파티션 키 = aggregate_id = ad_request_id)
 *   3) 발행에 성공한 id 를 published = true 로 업데이트
 *
 * 3)이 실패하면 그 배치는 다음 폴링에서 다시 1)에 걸려 재발행된다.
 * 즉 Kafka 에 중복이 생긴다. 버그가 아니라 Outbox 패턴의 정의된 성질이고(at-least-once),
 * 이 실습이 관찰하려는 대상이다. outbox.update-fail-rate 로 인위적으로 만든다.
 *
 * [여러 워커가 동시에 돌 때]
 * 1)~3)을 한 트랜잭션으로 묶고 1)에 FOR UPDATE SKIP LOCKED 를 붙인다.
 *   - FOR UPDATE   : 내가 고른 행을 커밋할 때까지 잠근다
 *   - SKIP LOCKED  : 남이 잠근 행은 기다리지 않고 건너뛴다 -> 워커마다 서로 다른 배치를 가져간다
 * 잠금이 없으면 두 워커가 같은 행을 동시에 읽어 둘 다 발행한다 (update-fail-rate 와 무관한 두 번째 중복원).
 * outbox.skip-locked=false 로 그 상황을 재현할 수 있다.
 *
 * 순서: 워커가 여럿이면 id 순서는 워커 사이에서 보장되지 않는다.
 * 여기서는 ad_request_id 하나당 outbox 행이 1개(ad_response)라 같은 키 안의 순서 문제가 없다.
 * 같은 키에 행이 여럿인 도메인이라면 키 단위로 워커를 나눠야 한다 (예: hash(aggregate_id) % N).
 *
 * 트랜잭션 안에서 Kafka 발행을 기다리므로 발행이 느리면 잠금도 그만큼 오래 잡힌다.
 * 그동안 다른 워커는 다음 행으로 넘어가므로 전체가 멈추지는 않는다.
 */
@Component
class OutboxWorker(
    private val jdbc: JdbcTemplate,
    private val kafka: KafkaTemplate<String, String>,
    private val metrics: OutboxMetrics,
    private val props: OutboxProperties,
    private val tx: TransactionTemplate,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val selectSql = """
        SELECT id, event_id, event_type, aggregate_id, payload::text, occurred_at
        FROM event_outbox
        WHERE published = false
        ORDER BY id
        LIMIT ?
    """.trimIndent() + (if (props.skipLocked) "\nFOR UPDATE SKIP LOCKED" else "")

    private val rowMapper = RowMapper { rs, _ ->
        OutboxRow(
            rs.getLong(1), rs.getString(2), rs.getString(3),
            rs.getString(4), rs.getString(5), rs.getTimestamp(6),
        )
    }

    private val backlogMapper = RowMapper { rs, _ -> rs.getLong(1) to rs.getLong(2) }

    /** 이미 한 번 발행한 id. 재발행(=중복) 건수를 세기 위한 것으로 동작에는 영향 없다. */
    private val seen = ConcurrentHashMap.newKeySet<Long>()
    private val totalPublished = AtomicLong(0)

    private lateinit var pool: ScheduledExecutorService

    @PostConstruct
    fun start() {
        val n = props.workers.coerceAtLeast(1)
        val seq = AtomicInteger(0)
        pool = Executors.newScheduledThreadPool(n) { r ->
            Thread(r, "outbox-${seq.incrementAndGet()}").apply { isDaemon = true }
        }
        repeat(n) {
            pool.scheduleWithFixedDelay(::pollSafely, 0, props.pollIntervalMs, TimeUnit.MILLISECONDS)
        }
        log.info(
            "outbox 워커 {}개 시작 (skip-locked={}, batch={}, update-fail-rate={})",
            n, props.skipLocked, props.batchSize, props.updateFailRate,
        )
        if (n > 1 && !props.skipLocked) {
            log.warn("워커가 {}개인데 skip-locked=false -> 같은 행을 여러 워커가 발행한다 (중복 재현 모드)", n)
        }
    }

    @PreDestroy
    fun stop() {
        pool.shutdown()
        pool.awaitTermination(10, TimeUnit.SECONDS)
    }

    /** 예외가 새어 나가면 scheduleWithFixedDelay 가 그 워커를 조용히 멈춘다. 그래서 여기서 막는다. */
    private fun pollSafely() {
        try {
            tx.executeWithoutResult { poll() }
        } catch (e: Exception) {
            log.warn("outbox 폴링 실패 (롤백 -> 다음 폴링에서 재시도): {}", e.toString())
        }
    }

    /** 트랜잭션 안에서 호출된다. 반환하면 커밋되고 잠금이 풀린다. */
    private fun poll() {
        val rows = jdbc.query(selectSql, rowMapper, props.batchSize)
        if (rows.isEmpty()) return

        val publishedIds = ArrayList<Long>(rows.size)
        var republished = 0
        for (row in rows) {
            try {
                kafka.send(props.topic, row.aggregateId, row.payload).get(5, TimeUnit.SECONDS)
                publishedIds += row.id
                if (!seen.add(row.id)) republished++
            } catch (e: Exception) {
                metrics.publishError(e::class.simpleName ?: "unknown")
                log.warn("outbox 발행 실패 id={} : {}", row.id, e.toString())
                break   // 순서를 지키기 위해 이 배치는 여기서 멈춘다
            }
        }
        if (seen.size > 200_000) seen.clear()
        if (publishedIds.isEmpty()) return

        metrics.published(publishedIds.size)
        totalPublished.addAndGet(publishedIds.size.toLong())
        if (republished > 0) {
            metrics.republished(republished)
            log.warn("outbox 재발행 {}건 -> Kafka 에 중복 발생", republished)
        }

        // ---- 중복이 태어나는 자리 ----------------------------------------
        // UPDATE 없이 커밋 -> 잠금만 풀리고 행은 미발행으로 남는다 -> 다음 폴링(어느 워커든)이 재발행
        if (ThreadLocalRandom.current().nextDouble() < props.updateFailRate) {
            metrics.updateSkipped(publishedIds.size)
            log.warn(
                "OUTBOX 플래그 업데이트 실패 재현: {}건 (id {}~{}) 을 미발행으로 남긴다 -> 다음 폴링에서 재발행",
                publishedIds.size, publishedIds.first(), publishedIds.last(),
            )
            return
        }

        // 진짜로 실패하면 예외가 트랜잭션을 롤백시키고 pollSafely 가 로그를 남긴다. 결과는 같다(재발행).
        jdbc.batchUpdate(
            "UPDATE event_outbox SET published = true, published_at = now() WHERE id = ?",
            publishedIds.map { arrayOf<Any>(it) },
        )
    }

    /** 미발행 건수와 최고 지연(가장 오래된 미발행 행의 나이)을 게이지에 반영 */
    @Scheduled(fixedDelayString = "\${outbox.stats-interval-ms:2000}")
    fun refreshBacklog() {
        try {
            val r = jdbc.query(
                """
                SELECT count(*)::bigint,
                       COALESCE(EXTRACT(EPOCH FROM (now() - min(occurred_at))), 0)::bigint
                FROM event_outbox WHERE published = false
                """.trimIndent(),
                backlogMapper,
            ).firstOrNull() ?: return
            metrics.setBacklog(r.first, r.second)
        } catch (e: Exception) {
            log.debug("outbox backlog 갱신 실패: {}", e.toString())
        }
    }

    fun totalPublished(): Long = totalPublished.get()
}
