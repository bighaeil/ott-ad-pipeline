package com.ottads.addecision.metrics

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

/**
 * /actuator/prometheus 로 노출.
 * Micrometer 가 카운터에 _total 을 붙이므로 이름에 직접 넣지 않는다.
 */
@Component
class OutboxMetrics(private val registry: MeterRegistry) {

    /** 아직 발행 안 된 outbox 행 수 */
    private val unpublished = AtomicLong(0)

    /** 미발행 행 중 가장 오래된 것의 지연(초) = 최고 지연 */
    private val maxLagSeconds = AtomicLong(0)

    init {
        registry.gauge("outbox.unpublished", unpublished) { it.get().toDouble() }
        registry.gauge("outbox.lag.seconds", maxLagSeconds) { it.get().toDouble() }
    }

    fun setBacklog(count: Long, lagSeconds: Long) {
        unpublished.set(count)
        maxLagSeconds.set(lagSeconds)
    }

    fun backlog(): Pair<Long, Long> = unpublished.get() to maxLagSeconds.get()

    fun published(n: Int) =
        registry.counter("outbox.published").increment(n.toDouble())

    /** 플래그 업데이트 실패로 같은 행을 다시 발행한 건수 = Kafka 에 생긴 중복 */
    fun republished(n: Int) =
        registry.counter("outbox.republished").increment(n.toDouble())

    fun updateSkipped(n: Int) =
        registry.counter("outbox.update.skipped").increment(n.toDouble())

    fun publishError(kind: String) =
        registry.counter("outbox.publish.errors", "kind", kind).increment()

    fun adRequest(fill: Boolean) =
        registry.counter("addecision.requests", "fill", fill.toString()).increment()
}
