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
    /** 한 프로세스 안에서 동시에 도는 폴링 워커 수. 인스턴스를 여러 개 띄운 것과 같은 경쟁을 만든다. */
    val workers: Int = 1,
    /** 조회에 FOR UPDATE SKIP LOCKED 를 붙일지. false + workers>1 이면 같은 행이 여러 번 발행된다. */
    val skipLocked: Boolean = true,
)
