package com.ottads.addecision.service

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

data class Campaign(
    val campaignId: String,
    val advertiser: String,
    val name: String,
    val vertical: String,
    val dailyBudget: Double,
    val cpm: Double,
)

/**
 * campaigns(+campaign_rates) 를 메모리에 올려 두고 예산 가중으로 하나를 고른다.
 *
 * 실제 광고 서버는 여기에 타게팅/빈도제어/페이싱/입찰이 들어간다.
 * 이 실습에서 관찰하려는 것은 "결정과 이벤트 기록이 한 트랜잭션인가" 이므로
 * 결정 로직 자체는 예산 가중 랜덤으로 축소했다.
 * 운영 구현과의 차이는 docs/03-ad-decision-api.md 의 비교표에 정리해 두었다.
 *
 * 캐시를 두는 이유: 광고 요청마다 DB 를 때리면 결정 지연(p99 목표 50ms)을 못 맞춘다.
 * 캠페인 메타는 분 단위로 바뀌어도 되는 데이터라 60초 주기 전체 갱신으로 충분하다.
 */
@Service
class CampaignCache(private val jdbc: JdbcTemplate) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val cache = AtomicReference<List<Campaign>>(emptyList())
    private val creatives = AtomicReference<Map<String, List<String>>>(emptyMap())

    @Scheduled(initialDelay = 0, fixedDelay = 60_000)
    fun refresh() {
        try {
            val rows = jdbc.query(
                """
                SELECT c.campaign_id, c.advertiser, c.name, c.vertical, c.daily_budget,
                       COALESCE(r.cpm, 0) AS cpm
                FROM campaigns c
                LEFT JOIN campaign_rates r ON r.campaign_id = c.campaign_id
                WHERE c.active = true
                """.trimIndent()
            ) { rs, _ ->
                Campaign(
                    rs.getString(1), rs.getString(2), rs.getString(3),
                    rs.getString(4), rs.getDouble(5), rs.getDouble(6),
                )
            }
            if (rows.isEmpty()) {
                log.warn("campaigns 테이블이 비어 있다. 노필만 나간다.")
            }
            cache.set(rows)
            // 소재는 캠페인당 2개로 고정 (별도 테이블을 두지 않았다).
            creatives.set(
                rows.associate {
                    it.campaignId to listOf(
                        "${it.campaignId.replace("cmp", "crt")}-a",
                        "${it.campaignId.replace("cmp", "crt")}-b",
                    )
                }
            )
        } catch (e: Exception) {
            log.warn("campaigns 갱신 실패: {}", e.toString())
        }
    }

    fun pick(): Campaign? {
        val list = cache.get()
        if (list.isEmpty()) return null
        val total = list.sumOf { it.dailyBudget }
        if (total <= 0) return list[ThreadLocalRandom.current().nextInt(list.size)]
        var r = ThreadLocalRandom.current().nextDouble() * total
        for (c in list) {
            r -= c.dailyBudget
            if (r <= 0) return c
        }
        return list.last()
    }

    /** 특정 캠페인을 지정해서 달라는 요청(데모/디버깅용)이 왔을 때. 없으면 null. */
    fun byId(campaignId: String?): Campaign? {
        if (campaignId.isNullOrBlank()) return null
        return cache.get().firstOrNull { it.campaignId == campaignId }
    }

    fun creativeFor(campaignId: String): String {
        val list = creatives.get()[campaignId] ?: return "crt-unknown"
        return list[ThreadLocalRandom.current().nextInt(list.size)]
    }

    /** GET /v1/campaigns 가 그대로 내보낸다. "지금 집행 중인 광고" 목록. */
    fun all(): List<Campaign> = cache.get()

    fun size(): Int = cache.get().size
}
