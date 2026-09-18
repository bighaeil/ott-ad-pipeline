package com.ottads.addecision.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

/**
 * Outbox 워커 전용 프로듀서.
 *
 * Collector 와 성격이 정반대다.
 *   Collector : 가용성 우선. 실패하면 파일에 흘리고 성공 응답 (fail-open).
 *   Outbox    : 정합성 우선. 실패하면 플래그를 안 올리고 다음 폴링에서 다시 보낸다.
 *               그래서 acks=all 로 확실히 받고, 유실 대신 중복을 택한다.
 */
@Configuration
class KafkaConfig {

    @Bean
    fun producerFactory(
        @Value("\${kafka.bootstrap}") bootstrap: String,
    ): ProducerFactory<String, String> {
        val props = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.CLIENT_ID_CONFIG to "outbox-worker",
            // 로컬은 브로커 1대라 all == 1 이다. 운영에서 min.insync.replicas=2 와 함께 의미가 생긴다.
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.LINGER_MS_CONFIG to 10,
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to 5000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to 5000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 10000,
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        )
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun kafkaTemplate(pf: ProducerFactory<String, String>): KafkaTemplate<String, String> =
        KafkaTemplate(pf)
}
