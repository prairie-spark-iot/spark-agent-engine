package com.spark.agent.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Spring Boot's default listener error handling retries a failing record 9x with no
 * delay, then logs and skips it with no trace — that's how diagnosis failures (DB
 * hiccups, Ollama timeouts) were silently vanishing from aiot_alert_record without
 * ever getting diagnosed. This gives every @KafkaListener two delayed retries, then
 * routes the record to a "<topic>.DLT" dead-letter topic instead of dropping it.
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 2));
        handler.setRetryListeners((consumerRecord, ex, deliveryAttempt) ->
                log.warn("[Kafka] Retry {} for topic={} partition={} offset={}: {}",
                        deliveryAttempt, consumerRecord.topic(), consumerRecord.partition(),
                        consumerRecord.offset(), ex.getMessage()));
        return handler;
    }
}
