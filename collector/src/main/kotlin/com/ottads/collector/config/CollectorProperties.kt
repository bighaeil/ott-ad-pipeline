package com.ottads.collector.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "collector")
data class CollectorProperties(
    /** /v1/track 서명 검증용 공유 비밀. 운영에서는 KMS/Secret Manager 에서 주입. */
    val hmacSecret: String = "local-dev-secret",
    /** Kafka 발행 실패 시 이벤트를 append 하는 디렉토리 (fail-open 경로). */
    val fallbackDir: String = "/data/fallback",
    /** 이 시간 안에 Kafka ack 가 오지 않으면 fallback 으로 넘긴다. */
    val publishTimeoutMs: Long = 3000,
    /** 한 번의 POST /v1/events 로 받을 수 있는 최대 이벤트 수. */
    val maxBatchSize: Int = 5000,
    /** 배치 내부 병렬 발행 수. 로컬 64 / 운영은 파티션 수에 맞춰 조정. */
    val publishConcurrency: Int = 64,
)
