package com.ottads.addecision.web

import com.ottads.addecision.metrics.OutboxMetrics
import com.ottads.addecision.service.AdRequest
import com.ottads.addecision.service.AdResponse
import com.ottads.addecision.service.Campaign
import com.ottads.addecision.service.CampaignCache
import com.ottads.addecision.service.DecisionService
import com.ottads.addecision.service.OutboxWorker
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

/**
 * 광고 결정 API.
 *
 *   POST /v1/ad-request     소재 결정 + 같은 트랜잭션에서 event_outbox INSERT
 *   GET  /v1/campaigns      지금 집행 중인 캠페인 (플레이어가 "무슨 광고가 있나" 를 보여줄 때)
 *   GET  /v1/outbox/stats   Outbox 적체/발행 현황
 *
 * 스펙 전문: docs/03-ad-decision-api.md
 */
@RestController
class AdRequestController(
    private val decision: DecisionService,
    private val worker: OutboxWorker,
    private val metrics: OutboxMetrics,
    private val campaigns: CampaignCache,
    private val jdbc: JdbcTemplate,
) {

    /** 광고 요청 -> 소재 결정 + 같은 트랜잭션에서 event_outbox INSERT */
    @PostMapping("/v1/ad-request")
    fun adRequest(@RequestBody req: AdRequest): AdResponse = decision.decide(req)

    data class CampaignList(val count: Int, val campaigns: List<Campaign>)

    /**
     * 캐시에 올라와 있는 캠페인 목록 (= 지금 광고로 나갈 수 있는 것들).
     * 60초 주기로 DB 에서 갱신되므로 방금 INSERT 한 캠페인은 최대 60초 늦게 보인다.
     */
    @GetMapping("/v1/campaigns")
    fun campaigns(): CampaignList = campaigns.all().let { CampaignList(it.size, it) }

    data class OutboxStats(
        val unpublished: Long,
        val maxLagSeconds: Long,
        val publishedTotalRuntime: Long,
        val rowsTotal: Long,
        val campaignsLoaded: Int,
    )

    /** 대시보드(단계 6)와 시나리오 스크립트가 읽는다. */
    @GetMapping("/v1/outbox/stats")
    fun outboxStats(): OutboxStats {
        val (unpub, lag) = metrics.backlog()
        val total = try {
            jdbc.queryForObject("SELECT count(*)::bigint FROM event_outbox", Long::class.java) ?: 0L
        } catch (e: Exception) {
            -1L
        }
        return OutboxStats(unpub, lag, worker.totalPublished(), total, campaigns.size())
    }
}
