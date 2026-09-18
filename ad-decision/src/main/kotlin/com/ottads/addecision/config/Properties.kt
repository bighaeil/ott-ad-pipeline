package com.ottads.addecision.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "addecision")
data class AdDecisionProperties(
    val noFillRate: Double = 0.08,
    /** 건너뛰기 버튼이 뜨기까지의 초. null 이면 건너뛸 수 없는 광고. */
    val skippableAfterS: Int = 5,
)

@ConfigurationProperties(prefix = "outbox")
data class OutboxProperties(
    val pollIntervalMs: Long = 500,
    val batchSize: Int = 500,
    val topic: String = "ad.request",
    /** 발행 후 플래그 업데이트 실패를 재현하는 확률. 0 이면 중복이 생기지 않는다. */
    val updateFailRate: Double = 0.02,
    val statsIntervalMs: Long = 2000,
)
