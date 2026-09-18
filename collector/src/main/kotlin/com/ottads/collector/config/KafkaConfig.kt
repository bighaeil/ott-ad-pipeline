package com.ottads.collector.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@Configuration
class KafkaConfig {

    @Bean
    fun producerFactory(
        @Value("\${kafka.bootstrap}") bootstrap: String,
        @Value("\${kafka.max-block-ms}") maxBlockMs: Int,
    ): ProducerFactory<String, String> {
        val props = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.CLIENT_ID_CONFIG to "collector",

            // acks=1 : 로컬은 브로커 1대라 all 과 차이가 없다. 운영은 acks=all + min.insync.replicas=2.
            ProducerConfig.ACKS_CONFIG to "1",

            // 수집기는 처리량 우선. 20ms 모아 보낸다.
            ProducerConfig.LINGER_MS_CONFIG to 20,
            ProducerConfig.BATCH_SIZE_CONFIG to 64 * 1024,
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
            ProducerConfig.BUFFER_MEMORY_CONFIG to 32L * 1024 * 1024,

            // fail-open 의 핵심 3종.
            // 브로커가 없을 때 send() 가 메타데이터를 기다리며 블로킹하는 상한이 max.block.ms 다.
            // 이 값이 크면 fallback 으로 넘어가는 게 늦어져 수집 지연으로 나타난다.
            ProducerConfig.MAX_BLOCK_MS_CONFIG to maxBlockMs,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to 2000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 4000,
            ProducerConfig.RETRIES_CONFIG to 1,          // 운영: 그대로 두고 delivery.timeout 으로 제어
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5,
            ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG to 2000,
        )
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun kafkaTemplate(pf: ProducerFactory<String, String>): KafkaTemplate<String, String> =
        KafkaTemplate(pf)
}
