package com.ottads.collector.service

import com.ottads.collector.model.Topics
import org.springframework.stereotype.Component

@Component
class TopicRouter {

    /** 알 수 없는 event_type 은 null -> dlq.invalid 로 간다. */
    fun topicFor(eventType: String): String? = when (eventType.lowercase()) {
        "impression", "ad_impression" -> Topics.IMPRESSION

        // VAST quartile 계열. 개별 타입으로 와도 하나의 토픽으로 모은다.
        "quartile", "start", "first_quartile", "firstquartile",
        "midpoint", "third_quartile", "thirdquartile", "complete" -> Topics.QUARTILE

        "click", "ad_click", "clickthrough" -> Topics.CLICK

        // 광고 요청/응답은 같은 토픽. 서버 확정 이벤트(Outbox)도 여기로 들어온다.
        "ad_request", "request", "ad_response", "response" -> Topics.REQUEST

        "progress", "play", "pause", "resume", "seek",
        "session_start", "session_end", "content_start", "content_end" -> Topics.BEHAVIOR

        else -> null
    }
}
