package com.ottads.collector.model

/**
 * 토픽 상수.
 * 파티션 키는 전부 ad_request_id 다 (없으면 event_id 로 폴백).
 * 하나의 광고 요청에서 파생된 request/impression/quartile/click 이 같은 파티션에 모이게 해
 * Flink 에서 ad_request_id 단위 상태 연산의 로컬리티를 확보한다.
 * 파티션 수: 로컬 6 / 운영 48.
 */
object Topics {
    const val IMPRESSION = "ad.impression"
    const val QUARTILE = "ad.quartile"
    const val CLICK = "ad.click"
    const val REQUEST = "ad.request"
    const val BEHAVIOR = "user.behavior"
    const val DLQ = "dlq.invalid"
}

/** 이벤트 1건 처리 결과. */
enum class Outcome {
    /** Kafka 로 정상 발행됨 */
    ACCEPTED,

    /** Kafka 발행 실패 -> 로컬 파일에 적재하고 성공 응답 (fail-open) */
    FALLBACK,

    /** 검증 실패 -> dlq.invalid 로 라우팅 */
    INVALID,
}

data class IngestResponse(
    val received: Int,
    val accepted: Int,
    val invalid: Int,
    val fallback: Int,
    val tookMs: Long,
)
