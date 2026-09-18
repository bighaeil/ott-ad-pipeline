package com.ottads.collector.service

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

sealed interface Validation {
    data class Ok(val eventTime: Instant) : Validation
    data class Fail(val reason: String) : Validation
}

/**
 * 필수 필드: event_id, event_type, campaign_id, event_time
 * event_time 은 ISO-8601 문자열 또는 epoch millis 를 받아 Instant 로 정규화한다.
 *
 * 여기서 지연 이벤트(event_time 이 과거)는 걸러내지 않는다.
 * 늦게 온 것과 잘못된 것은 다른 문제라서, 지연 판정은 Flink 워터마크가 담당한다.
 */
@Component
class EventValidator {

    companion object {
        val REQUIRED = listOf("event_id", "event_type", "campaign_id", "event_time")
        /** 이보다 먼 미래 타임스탬프는 클라이언트 시계 오류로 본다. */
        const val MAX_FUTURE_SKEW_SECONDS = 300L
    }

    fun validate(node: JsonNode?): Validation {
        if (node == null || !node.isObject) return Validation.Fail("not_an_object")

        for (field in REQUIRED) {
            val v = node.get(field)
            if (v == null || v.isNull) return Validation.Fail("missing_field:$field")
            if (v.isTextual && v.asText().isBlank()) return Validation.Fail("blank_field:$field")
        }

        val et = parseInstant(node.get("event_time"))
            ?: return Validation.Fail("bad_event_time")

        if (et.isAfter(Instant.now().plusSeconds(MAX_FUTURE_SKEW_SECONDS))) {
            return Validation.Fail("event_time_in_future")
        }
        return Validation.Ok(et)
    }

    fun parseInstant(node: JsonNode?): Instant? {
        if (node == null || node.isNull) return null
        return try {
            when {
                node.isNumber -> Instant.ofEpochMilli(node.asLong())
                node.isTextual -> parseInstant(node.asText())
                else -> null
            }
        } catch (e: Exception) {
            null
        }
    }

    fun parseInstant(raw: String?): Instant? {
        if (raw.isNullOrBlank()) return null
        raw.toLongOrNull()?.let { return Instant.ofEpochMilli(it) }
        return try {
            OffsetDateTime.parse(raw).toInstant()
        } catch (e: DateTimeParseException) {
            try {
                Instant.parse(raw)
            } catch (e2: DateTimeParseException) {
                null
            }
        }
    }
}
