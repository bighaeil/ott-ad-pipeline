package com.ottads.addecision.service

import com.ottads.addecision.config.OutboxProperties
import com.ottads.addecision.metrics.OutboxMetrics
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.sql.Timestamp
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
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
 * 로컬은 워커가 1개라 잠금이 필요 없다.
 * 운영에서 인스턴스를 여러 개 띄우면 조회에 FOR UPDATE SKIP LOCKED 를 붙여야 한다.
 */
@Component
class OutboxWorker(
    private val jdbc: JdbcTemplate,
    private val kafka: KafkaTemplate<String, String>,
    private val metrics: OutboxMetrics,
    private val props: OutboxProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val selectSql = """
        SELECT id, event_id, event_type, aggregate_id, payload::text, occurred_at
        FROM event_outbox
        WHERE published = false
        ORDER BY id
        LIMIT ?
    """.trimIndent()
    // 운영(다중 워커): 위 쿼리 끝에 FOR UPDATE SKIP LOCKED 를 붙이고 트랜잭션 안에서 돌린다.

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

    @Scheduled(fixedDelayString = "\${outbox.poll-interval-ms:500}")
    fun poll() {
        val rows = try {
            jdbc.query(selectSql, rowMapper, props.batchSize)
        } catch (e: Exception) {
            log.warn("outbox 조회 실패: {}", e.toString())
            return
        }
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
            log.warn("outbox 재발행 {}건 (직전 업데이트 실패분) -> Kafka 에 중복 발생", republished)
        }

        // ---- 중복이 태어나는 자리 ----------------------------------------
        if (ThreadLocalRandom.current().nextDouble() < props.updateFailRate) {
            metrics.updateSkipped(publishedIds.size)
            log.warn(
                "OUTBOX 플래그 업데이트 실패 재현: {}건 (id {}~{}) 을 미발행으로 남긴다 -> 다음 폴링에서 재발행",
                publishedIds.size, publishedIds.first(), publishedIds.last(),
            )
            return
        }

        try {
            jdbc.batchUpdate(
                "UPDATE event_outbox SET published = true, published_at = now() WHERE id = ?",
                publishedIds.map { arrayOf<Any>(it) },
            )
        } catch (e: Exception) {
            // 진짜로 실패해도 결과는 같다. 다음 폴링에서 재발행된다.
            metrics.updateSkipped(publishedIds.size)
            log.error("outbox 플래그 업데이트 실패 ({}건) -> 재발행 예정: {}", publishedIds.size, e.toString())
        }
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
