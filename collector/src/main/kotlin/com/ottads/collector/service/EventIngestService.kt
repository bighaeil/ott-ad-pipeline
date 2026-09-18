package com.ottads.collector.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.ottads.collector.metrics.CollectorMetrics
import com.ottads.collector.model.Outcome
import com.ottads.collector.model.Topics
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * 검증 -> 보강 -> 라우팅 -> 발행.
 * POST /v1/events 와 GET /v1/track 이 공유한다.
 */
@Service
class EventIngestService(
    private val validator: EventValidator,
    private val router: TopicRouter,
    private val publisher: EventPublisher,
    private val metrics: CollectorMetrics,
    private val mapper: ObjectMapper,
) {

    fun ingest(node: JsonNode?, endpoint: String): Mono<Outcome> {
        when (val v = validator.validate(node)) {
            is Validation.Fail -> return toDlq(node, v.reason, endpoint)
            is Validation.Ok -> {
                val obj = node as ObjectNode
                val eventType = obj.get("event_type").asText()
                val topic = router.topicFor(eventType)
                    ?: return toDlq(obj, "unknown_event_type:$eventType", endpoint)

                enrich(obj, v.eventTime, endpoint)

                // 파티션 키 = ad_request_id. 없으면 event_id 로 폴백.
                // 같은 광고 요청의 이벤트가 한 파티션에 모여야 Flink 상태 연산이 로컬해진다.
                val key = obj.get("ad_request_id")?.takeIf { !it.isNull }?.asText()
                    ?: obj.get("event_id").asText()

                return publisher.publish(topic, key, mapper.writeValueAsString(obj))
            }
        }
    }

    private fun enrich(obj: ObjectNode, eventTime: Instant, endpoint: String) {
        // 수집 서버가 찍는 시각. event_time(발생 시각) 과의 차이가 곧 지연이다.
        // 밀리초로 자른다. Instant.toString() 은 나노초까지 찍는데
        // Flink JSON 포맷(ISO-8601, TIMESTAMP(3))이 9자리 소수를 못 읽는다.
        obj.put("server_ts", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString())
        obj.put("event_time", eventTime.truncatedTo(ChronoUnit.MILLIS).toString())  // ISO-8601 UTC(밀리초)
        obj.put("ingest_endpoint", endpoint)
        if (!obj.has("source")) obj.put("source", "client")
    }

    /** 검증 실패 이벤트. 버리지 않고 dlq.invalid 로 보내 원인을 나중에 셀 수 있게 한다. */
    fun toDlq(original: JsonNode?, reason: String, endpoint: String): Mono<Outcome> {
        metrics.invalid(reason.substringBefore(':'))
        val dlq = mapper.createObjectNode().apply {
            put("dlq_id", UUID.randomUUID().toString())
            put("reason", reason)
            put("endpoint", endpoint)
            put("server_ts", Instant.now().toString())
            set<JsonNode>("raw", original ?: mapper.nullNode())
        }
        val key = original?.get("event_id")?.asText()?.takeIf { it.isNotBlank() }
            ?: dlq.get("dlq_id").asText()

        metrics.dlq(reason.substringBefore(':'))
        return publisher.publish(Topics.DLQ, key, mapper.writeValueAsString(dlq))
            .map { Outcome.INVALID }   // Kafka 성공/fallback 여부와 무관하게 "검증 실패" 로 집계
    }
}
