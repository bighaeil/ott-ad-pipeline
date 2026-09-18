package com.ottads.addecision.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.ottads.addecision.config.AdDecisionProperties
import com.ottads.addecision.metrics.OutboxMetrics
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * POST /v1/ad-request 요청 본문.
 *
 * 전부 선택값이다. 플레이어가 줄 수 있는 것만 주고, 없으면 서버가 채운다.
 * 필드 정의와 운영 확장(타게팅/빈도제어/RTB)은 docs/03-ad-decision-api.md 참조.
 */
data class AdRequest(
    /** 광고 1편의 식별자. Kafka 파티션 키가 된다. 없으면 서버가 발급한다. */
    val ad_request_id: String? = null,
    val session_id: String? = null,
    val user_id: String? = null,
    val content_id: String? = null,
    /** smart_tv | mobile | tablet | pc | stb */
    val device: String? = null,
    /** 광고 팟(연속 광고 묶음) 식별자 */
    val ad_pod_id: String? = null,
    /** 팟 안에서 몇 번째 광고인지 (0부터) */
    val ad_slot: Int = 0,
    /** 광고가 끼어드는 콘텐츠 재생 위치(초) */
    val playhead_s: Double = 0.0,
    /** 이 슬롯이 허용하는 최대 광고 길이(초) */
    val max_duration_s: Int = 30,
    // ---- 아래 둘은 데모/디버깅용. 운영 API 에는 없는 필드다. -------------
    /** 이 캠페인으로 채워 달라 (없거나 비활성이면 무시하고 정상 선택) */
    val prefer_campaign_id: String? = null,
    /** true 면 노필을 강제한다. 플레이어에서 노필 화면을 재현할 때 쓴다. */
    val force_no_fill: Boolean = false,
)

/** VAST 트래킹 이벤트 하나. 플레이어는 offset_pct 지점에서 해당 비콘을 쏜다. */
data class TrackingEvent(
    val event: String,
    val quartile: String?,
    val offset_pct: Double,
)

data class AdResponse(
    val ad_request_id: String,
    val fill: Boolean,
    /** 노필일 때만 채워진다: no_campaign | budget_exhausted | policy */
    val no_fill_reason: String?,
    val campaign_id: String?,
    val campaign_name: String?,
    val advertiser: String?,
    val vertical: String?,
    /** 1000 임프레션당 단가(KRW). 정산은 Spark 배치가 이 값으로 한다. */
    val cpm: Double?,
    val creative_id: String?,
    val ad_duration_s: Int?,
    /** 이 초를 넘기면 건너뛰기 버튼이 뜬다. null 이면 건너뛸 수 없다. */
    val skippable_after_s: Int?,
    val click_through_url: String?,
    /** 플레이어가 쏴야 하는 비콘 목록 (VAST TrackingEvents 축약) */
    val tracking_events: List<TrackingEvent>,
    val decision_ms: Long,
)

@Service
class DecisionService(
    private val jdbc: JdbcTemplate,
    private val campaigns: CampaignCache,
    private val mapper: ObjectMapper,
    private val props: AdDecisionProperties,
    private val metrics: OutboxMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val insertSql = """
        INSERT INTO event_outbox (event_id, event_type, aggregate_id, payload, occurred_at)
        VALUES (?, ?, ?, ?::jsonb, ?)
    """.trimIndent()

    /** 플레이어가 쏘는 비콘. Collector 의 TopicRouter 가 아는 이름과 맞춰 둔다. */
    private val trackingEvents = listOf(
        TrackingEvent("impression", null, 0.0),
        TrackingEvent("quartile", "start", 0.0),
        TrackingEvent("quartile", "first_quartile", 0.25),
        TrackingEvent("quartile", "midpoint", 0.50),
        TrackingEvent("quartile", "third_quartile", 0.75),
        TrackingEvent("quartile", "complete", 1.0),
    )

    /**
     * 소재 결정과 event_outbox INSERT 가 같은 트랜잭션이다. 이게 Outbox 패턴의 전부다.
     *
     * Kafka 발행을 여기서 하지 않는 이유:
     *   DB 커밋과 Kafka 발행은 하나의 원자 단위가 될 수 없다.
     *   여기서 발행하면 "커밋은 됐는데 발행 실패" 또는 "발행은 됐는데 롤백" 이 생긴다.
     *   그래서 같은 트랜잭션에는 DB 쓰기만 넣고, 발행은 워커가 따로 한다.
     *   대가는 중복이다 (at-least-once). 유실보다 중복이 낫다는 선택.
     */
    @Transactional
    fun decide(req: AdRequest): AdResponse {
        val t0 = System.nanoTime()
        val arid = req.ad_request_id ?: ("req-" + UUID.randomUUID().toString().replace("-", "").take(16))

        // 1) 채울지 말지. 운영이라면 예산 소진/타게팅 불일치/정책 차단이 이 자리다.
        val wantFill = !req.force_no_fill &&
            ThreadLocalRandom.current().nextDouble() >= props.noFillRate
        // 2) 어떤 캠페인으로 채울지. 지정이 있으면 그것, 없으면 예산 가중 랜덤.
        val campaign = if (wantFill) (campaigns.byId(req.prefer_campaign_id) ?: campaigns.pick()) else null
        val filled = campaign != null
        val noFillReason = when {
            filled -> null
            req.force_no_fill -> "policy"
            campaigns.size() == 0 -> "no_campaign"
            else -> "budget_exhausted"
        }
        val creative = campaign?.let { campaigns.creativeFor(it.campaignId) }
        val duration = if (filled) {
            if (req.max_duration_s >= 30 && ThreadLocalRandom.current().nextBoolean()) 30 else 15
        } else null

        // Outbox 에 넣는 것은 "서버가 확정한 응답" 이다.
        // 클라이언트가 보내는 ad_request 와 짝을 이뤄 같은 ad.request 토픽에 들어간다.
        // 둘의 개수 차이가 곧 유실/노필이고, 단계 4의 정합성 지표가 이걸 본다.
        val eventId = "evt-outbox-" + UUID.randomUUID().toString().replace("-", "").take(16)
        val payload = mapper.writeValueAsString(
            buildMap<String, Any?> {
                put("event_id", eventId)
                put("event_type", "ad_response")
                put("campaign_id", campaign?.campaignId ?: "nofill")
                // 밀리초로 자른다 (Flink JSON TIMESTAMP(3) 파서 제약)
                put("event_time", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString())
                put("ad_request_id", arid)
                put("creative_id", creative)
                put("ad_duration_s", duration)
                put("fill", filled)
                put("session_id", req.session_id)
                put("user_id", req.user_id)
                put("content_id", req.content_id)
                put("device", req.device)
                put("ad_pod_id", req.ad_pod_id)
                put("ad_slot", req.ad_slot)
                put("playhead_s", req.playhead_s)
                put("source", "server")
                put("transport", "outbox")
            }
        )

        jdbc.update(
            insertSql,
            eventId,
            "ad_response",
            arid,                                   // = Kafka 파티션 키
            payload,
            Timestamp.from(Instant.now()),
        )

        metrics.adRequest(filled)
        return AdResponse(
            ad_request_id = arid,
            fill = filled,
            no_fill_reason = noFillReason,
            campaign_id = campaign?.campaignId,
            campaign_name = campaign?.name,
            advertiser = campaign?.advertiser,
            vertical = campaign?.vertical,
            cpm = campaign?.cpm,
            creative_id = creative,
            ad_duration_s = duration,
            skippable_after_s = if (filled) props.skippableAfterS else null,
            click_through_url = campaign?.let { "https://ads.example.com/click/${it.campaignId}?arid=$arid" },
            tracking_events = if (filled) trackingEvents else emptyList(),
            decision_ms = (System.nanoTime() - t0) / 1_000_000,
        )
    }
}
