package com.spark.agent.service;

import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * The Kafka producer is transactional (spring.kafka.producer.transaction-id-prefix), so
 * kafkaTemplate.send() inside this @Transactional method synchronizes with the JPA
 * transaction instead of firing immediately: the Kafka transaction commits first, then the
 * published_at write — if either fails, both roll back, so a row can never end up sent but
 * not marked published (or vice versa). Must be a separate bean from OutboxRelayService so
 * the @Transactional proxy applies (a self-invoked call would bypass it).
 */
@Service
@RequiredArgsConstructor
public class OutboxPublisher {

    private final KafkaProducerService kafkaProducerService;
    private final OutboxMessageRepository outboxMessageRepository;

    @Transactional
    public void publish(OutboxMessage msg, String topic, String key) {
        kafkaProducerService.sendRaw(topic, key, msg.getPayload());
        outboxMessageRepository.markPublished(msg.getId(), LocalDateTime.now());
    }
}
