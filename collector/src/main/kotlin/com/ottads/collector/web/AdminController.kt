package com.ottads.collector.web

import com.fasterxml.jackson.databind.ObjectMapper
import com.ottads.collector.config.CollectorProperties
import com.ottads.collector.metrics.CollectorMetrics
import com.ottads.collector.model.Outcome
import com.ottads.collector.service.EventPublisher
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name

/**
 * 운영 도구. 실제 환경이라면 별도 포트/인증 뒤에 둔다.
 * 여기서는 시나리오 스크립트가 호출할 수 있게 같은 포트에 노출한다.
 */
@RestController
@RequestMapping("/v1/admin")
class AdminController(
    private val props: CollectorProperties,
    private val publisher: EventPublisher,
    private val metrics: CollectorMetrics,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    data class FallbackStatus(val files: Int, val lines: Long, val dir: String)
    data class ReplayResult(val files: Int, val replayed: Long, val failed: Long)

    @GetMapping("/fallback")
    fun status(): Mono<FallbackStatus> = Mono.fromCallable {
        val dir = Path.of(props.fallbackDir)
        if (!Files.isDirectory(dir)) return@fromCallable FallbackStatus(0, 0, dir.toString())
        val files = pendingFiles(dir)
        val lines = files.sumOf { f -> Files.lines(f).use { it.count() } }
        FallbackStatus(files.size, lines, dir.toString())
    }.subscribeOn(Schedulers.boundedElastic())

    /**
     * Kafka 가 살아난 뒤 fallback 파일을 다시 밀어 넣는다.
     * 성공한 파일은 .done 으로 rename 한다 (지우지 않고 남겨 관찰 가능하게).
     *
     * 재적재는 중복을 만든다. 같은 event_id 가 이미 갔을 수도 있기 때문.
     * 그 중복을 Flink(1시간 TTL)와 Spark(전체 범위)가 각각 어떻게 처리하는지가
     * 이 파이프라인에서 볼 만한 지점이다.
     */
    @PostMapping("/fallback/replay")
    fun replay(): Mono<ResponseEntity<ReplayResult>> = Mono.fromCallable {
        val dir = Path.of(props.fallbackDir)
        if (!Files.isDirectory(dir)) emptyList() else pendingFiles(dir)
    }.subscribeOn(Schedulers.boundedElastic())
        .flatMap { files ->
            if (files.isEmpty()) {
                return@flatMap Mono.just(ResponseEntity.ok(ReplayResult(0, 0, 0)))
            }
            log.warn("fallback replay start: {} file(s)", files.size)
            Flux.fromIterable(files)
                .concatMap { file -> replayFile(file) }
                .reduce(longArrayOf(0, 0)) { acc, r -> acc[0] += r[0]; acc[1] += r[1]; acc }
                .map { acc ->
                    metrics.fallbackDrained(acc[0])
                    log.warn("fallback replay done: replayed={} failed={}", acc[0], acc[1])
                    ResponseEntity.ok(ReplayResult(files.size, acc[0], acc[1]))
                }
        }

    private fun replayFile(file: Path): Mono<LongArray> =
        Mono.fromCallable { Files.readAllLines(file) }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapMany { Flux.fromIterable(it) }
            .filter { it.isNotBlank() }
            .flatMap({ line ->
                val n = mapper.readTree(line)
                publisher.publish(
                    n.get("topic").asText(),
                    n.get("key").asText(),
                    n.get("payload").asText(),
                )
            }, 32)
            .reduce(longArrayOf(0, 0)) { acc, outcome ->
                if (outcome == Outcome.ACCEPTED) acc[0]++ else acc[1]++
                acc
            }
            .flatMap { acc ->
                Mono.fromCallable {
                    // 전부 성공했을 때만 .done 처리. 하나라도 실패하면 파일을 남겨 다음 시도를 기다린다.
                    if (acc[1] == 0L) {
                        Files.move(file, file.resolveSibling(file.name + ".done"))
                    }
                    acc
                }.subscribeOn(Schedulers.boundedElastic())
            }

    private fun pendingFiles(dir: Path): List<Path> =
        Files.list(dir).use { s ->
            s.filter { it.name.startsWith("fallback-") && it.name.endsWith(".jsonl") }
                .sorted()
                .toList()
        }
}
