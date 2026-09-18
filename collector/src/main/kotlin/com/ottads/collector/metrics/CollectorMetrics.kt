package com.ottads.collector.metrics

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

/**
 * Prometheus 노출은 /actuator/prometheus.
 * Micrometer 가 카운터 이름 끝에 _total 을 붙이므로 여기서는 붙이지 않는다.
 *   collector.events.received  ->  collector_events_received_total{endpoint="..."}
 */
@Component
class CollectorMetrics(private val registry: MeterRegistry) {

    private val fallbackBacklog = AtomicLong(0)

    init {
        registry.gauge("collector.fallback.pending", fallbackBacklog) { it.get().toDouble() }
    }

    fun received(endpoint: String, n: Int = 1) =
        registry.counter("collector.events.received", "endpoint", endpoint).increment(n.toDouble())

    fun accepted(topic: String) =
        registry.counter("collector.events.accepted", "topic", topic).increment()

    /** 검증 실패 (필수 필드 누락 / 서명 위조 / 만료 / 알 수 없는 event_type) */
    fun invalid(reason: String) =
        registry.counter("collector.events.invalid", "reason", reason).increment()

    /** dlq.invalid 토픽으로 실제 실린 건수 */
    fun dlq(reason: String) =
        registry.counter("collector.dlq", "reason", reason).increment()

    /** Kafka 발행 실패 -> 로컬 파일로 흘린 건수 */
    fun fallback(topic: String) {
        registry.counter("collector.events.fallback", "topic", topic).increment()
        fallbackBacklog.incrementAndGet()
    }

    fun kafkaError(kind: String) =
        registry.counter("collector.kafka.publish.errors", "kind", kind).increment()

    /** fallback 파일을 재적재했을 때 backlog 게이지를 되돌린다. */
    fun fallbackDrained(n: Long) = fallbackBacklog.addAndGet(-n)
}
