package com.ottads.collector.web

import com.fasterxml.jackson.databind.ObjectMapper
import com.ottads.collector.metrics.CollectorMetrics
import com.ottads.collector.service.EventIngestService
import com.ottads.collector.service.EventValidator
import com.ottads.collector.service.SignatureVerifier
import org.slf4j.LoggerFactory
import org.springframework.http.CacheControl
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.Base64

/**
 * VAST 트래킹 픽셀.
 *
 * 파라미터
 *   eid  event_id        (필수)
 *   et   event_type      (필수, 예: impression / quartile / click)
 *   cid  campaign_id     (필수)
 *   ts   event_time      (필수, epoch millis 또는 ISO-8601)
 *   arid ad_request_id   (권장, 파티션 키)
 *   q    quartile        (et=quartile 일 때)
 *   src  source          (client | server, 기본 client)
 *   exp  만료 epoch seconds (필수)
 *   sig  HMAC-SHA256 hex  (필수)
 *
 * 검증에 실패해도 HTTP 는 항상 200 + 1x1 GIF 다.
 * 픽셀 응답이 깨지면 플레이어가 재시도 폭주를 일으키므로, 실패는 dlq.invalid 로만 남긴다.
 */
@RestController
class TrackController(
    private val verifier: SignatureVerifier,
    private val ingest: EventIngestService,
    private val validator: EventValidator,
    private val metrics: CollectorMetrics,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        /** 1x1 투명 GIF */
        private val PIXEL: ByteArray = Base64.getDecoder()
            .decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
        private const val ENDPOINT = "v1_track"
    }

    @GetMapping("/v1/track")
    fun track(exchange: ServerWebExchange): Mono<ResponseEntity<ByteArray>> {
        metrics.received(ENDPOINT)
        val params: Map<String, List<String>> = exchange.request.queryParams

        val work: Mono<*> = when (val r = verifier.verify(params)) {
            is SignatureVerifier.Result.Invalid -> {
                val raw = mapper.createObjectNode().apply {
                    params.forEach { (k, v) -> put(k, v.firstOrNull()) }
                    put("_query", exchange.request.uri.rawQuery ?: "")
                }
                log.debug("track signature rejected: {} q={}", r.reason, exchange.request.uri.rawQuery)
                ingest.toDlq(raw, "track_${r.reason}", ENDPOINT)
            }

            SignatureVerifier.Result.Valid -> {
                val node = mapper.createObjectNode().apply {
                    put("event_id", params["eid"]?.firstOrNull())
                    put("event_type", params["et"]?.firstOrNull() ?: "impression")
                    put("campaign_id", params["cid"]?.firstOrNull())
                    put(
                        "event_time",
                        validator.parseInstant(params["ts"]?.firstOrNull())?.toString()
                            ?: Instant.now().toString(),
                    )
                    put("ad_request_id", params["arid"]?.firstOrNull())
                    put("creative_id", params["crid"]?.firstOrNull())
                    put("quartile", params["q"]?.firstOrNull())
                    put("session_id", params["sid"]?.firstOrNull())
                    // SSAI 스티처가 서버 경로로 보낸 것과 클라이언트 경로를 구분하는 필드.
                    // 단계 5 의 SSAI 중복 제거가 이 값의 우선순위로 하나를 고른다.
                    put("source", params["src"]?.firstOrNull() ?: "client")
                    put("transport", "pixel")
                }
                ingest.ingest(node, ENDPOINT)
            }
        }

        return work.thenReturn(pixelResponse()).onErrorReturn(pixelResponse())
    }

    private fun pixelResponse(): ResponseEntity<ByteArray> =
        ResponseEntity.ok()
            .contentType(MediaType.IMAGE_GIF)
            .cacheControl(CacheControl.noStore().mustRevalidate())
            .header("Pragma", "no-cache")
            .body(PIXEL)
}
