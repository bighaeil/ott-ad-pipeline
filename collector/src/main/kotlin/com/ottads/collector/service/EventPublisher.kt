package com.ottads.collector.service

import com.ottads.collector.config.CollectorProperties
import com.ottads.collector.metrics.CollectorMetrics
import com.ottads.collector.model.Outcome
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * Kafka 발행 + fail-open.
 *
 * 중요한 두 가지:
 *  1) KafkaTemplate.send() 는 메타데이터가 없으면 max.block.ms 만큼 "호출 스레드를 블로킹"한다.
 *     WebFlux 이벤트 루프에서 이걸 그대로 부르면 전체 서버가 멈춘다.
 *     그래서 subscribeOn(boundedElastic) 으로 반드시 워커 스레드에서 실행한다.
 *  2) 실패하면 예외를 올리지 않고 로컬 파일에 적고 성공으로 처리한다 (fail-open).
 *     수집기는 광고 재생을 막으면 안 되므로 가용성 > 정합성.
 */
@Service
class EventPublisher(
    private val kafka: KafkaTemplate<String, String>,
    private val fallback: FallbackWriter,
    private val metrics: CollectorMetrics,
    private val props: CollectorProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val timeout = Duration.ofMillis(props.publishTimeoutMs)
    private val failStreak = AtomicLong(0)

    fun publish(topic: String, key: String, payload: String): Mono<Outcome> =
        Mono.fromFuture { kafka.send(topic, key, payload) }
            .subscribeOn(Schedulers.boundedElastic())
            .timeout(timeout)
            .map<Outcome> {
                if (failStreak.getAndSet(0) > 0) {
                    log.warn("kafka publish RECOVERED topic={}", topic)
                }
                metrics.accepted(topic)
                Outcome.ACCEPTED
            }
            .onErrorResume { e ->
                val kind = e::class.simpleName ?: "unknown"
                metrics.kafkaError(kind)
                val streak = failStreak.incrementAndGet()
                if (streak == 1L || streak % 500L == 0L) {
                    log.warn(
                        "FAIL-OPEN kafka publish failed (streak={}) topic={} err={}: {} -> writing to fallback file",
                        streak, topic, kind, e.message,
                    )
                }
                Mono.fromCallable {
                    fallback.append(topic, key, payload)
                    metrics.fallback(topic)
                    Outcome.FALLBACK
                }.subscribeOn(Schedulers.boundedElastic())
            }
}
