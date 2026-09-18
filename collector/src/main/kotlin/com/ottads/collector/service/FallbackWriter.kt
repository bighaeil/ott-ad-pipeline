package com.ottads.collector.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.ottads.collector.config.CollectorProperties
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

/**
 * Kafka 발행 실패 시의 fail-open 경로.
 *
 * 한 줄 = 하나의 이벤트 (JSON Lines).
 *   {"topic":"ad.impression","key":"<ad_request_id>","payload":"<원본 JSON 문자열>","fallback_ts":"..."}
 *
 * payload 를 문자열로 감싸는 이유: 재적재 스크립트가 파싱 없이 그대로 Kafka 에 다시 밀 수 있게.
 *
 * 운영이라면 이 자리는 로컬 디스크가 아니라 사이드카 -> S3, 또는 로컬 Kafka(mirror) 다.
 * 로컬 파일은 컨테이너가 죽으면 같이 사라지므로 볼륨(./data/fallback)에 붙여 둔다.
 */
@Component
class FallbackWriter(
    private val props: CollectorProperties,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val lock = Any()
    private val hourFmt = DateTimeFormatter.ofPattern("yyyyMMdd-HH").withZone(ZoneOffset.UTC)
    private val written = AtomicLong(0)

    fun append(topic: String, key: String, payload: String) {
        val now = Instant.now()
        val line = mapper.writeValueAsString(
            mapOf(
                "topic" to topic,
                "key" to key,
                "payload" to payload,
                "fallback_ts" to now.toString(),
            )
        )
        try {
            val dir = Path.of(props.fallbackDir)
            Files.createDirectories(dir)
            val file: Path = dir.resolve("fallback-${hourFmt.format(now)}.jsonl")
            synchronized(lock) {
                Files.write(
                    file,
                    (line + "\n").toByteArray(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                )
            }
            val n = written.incrementAndGet()
            if (n == 1L || n % 500L == 0L) {
                log.warn("FAIL-OPEN fallback file append: total={} file={} topic={}", n, file, topic)
            }
        } catch (e: Exception) {
            // 파일까지 실패하면 여기서 이벤트가 유실된다. 유일하게 유실이 발생하는 지점.
            log.error("FAIL-OPEN fallback WRITE FAILED (event lost) topic={} key={} err={}", topic, key, e.toString())
        }
    }

    fun totalWritten(): Long = written.get()
}
