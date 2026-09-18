package com.ottads.collector.web

import com.fasterxml.jackson.databind.JsonNode
import com.ottads.collector.config.CollectorProperties
import com.ottads.collector.metrics.CollectorMetrics
import com.ottads.collector.model.IngestResponse
import com.ottads.collector.model.Outcome
import com.ottads.collector.service.EventIngestService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
class EventController(
    private val ingest: EventIngestService,
    private val metrics: CollectorMetrics,
    private val props: CollectorProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 배치 이벤트 수신.
     * body 는 JSON 배열 `[{...},{...}]` 또는 `{"events":[...]}` 둘 다 받는다.
     *
     * 응답은 항상 202. 개별 이벤트가 검증 실패해도 배치 전체를 거절하지 않는다
     * (플레이어 SDK 가 배치 재전송을 반복하면 폭주하므로).
     */
    @PostMapping("/v1/events", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun events(@RequestBody body: Mono<JsonNode>): Mono<ResponseEntity<IngestResponse>> {
        val started = System.nanoTime()
        return body.flatMap { root ->
            val list: List<JsonNode> = when {
                root.isArray -> root.toList()
                root.isObject && root.has("events") && root.get("events").isArray ->
                    root.get("events").toList()
                root.isObject -> listOf(root)          // 단건도 허용
                else -> emptyList()
            }

            if (list.size > props.maxBatchSize) {
                log.warn("batch too large: {} > {}", list.size, props.maxBatchSize)
                return@flatMap Mono.just(
                    ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                        .body(IngestResponse(list.size, 0, list.size, 0, 0))
                )
            }

            metrics.received("v1_events", list.size)

            Flux.fromIterable(list)
                .flatMap({ ingest.ingest(it, "v1_events") }, props.publishConcurrency)
                .reduce(intArrayOf(0, 0, 0)) { acc, outcome ->
                    when (outcome) {
                        Outcome.ACCEPTED -> acc[0]++
                        Outcome.INVALID -> acc[1]++
                        Outcome.FALLBACK -> acc[2]++
                    }
                    acc
                }
                .defaultIfEmpty(intArrayOf(0, 0, 0))
                .map { acc ->
                    ResponseEntity.accepted().body(
                        IngestResponse(
                            received = list.size,
                            accepted = acc[0],
                            invalid = acc[1],
                            fallback = acc[2],
                            tookMs = (System.nanoTime() - started) / 1_000_000,
                        )
                    )
                }
        }
    }
}
