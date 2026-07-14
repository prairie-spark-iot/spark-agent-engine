package com.spark.agent.service;

import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private KafkaProducerService kafkaProducerService;
    @Mock
    private OutboxMessageRepository outboxMessageRepository;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(kafkaProducerService, outboxMessageRepository);
    }

    @Test
    void publish_sendsThenMarksPublished() {
        OutboxMessage msg = new OutboxMessage();
        msg.setId(1L);
        msg.setPayload("{\"deviceKey\":\"DK_TEST_001\"}");

        publisher.publish(msg, "iot.device.data", "DK_TEST_001");

        verify(kafkaProducerService).sendRaw("iot.device.data", "DK_TEST_001", msg.getPayload());
        verify(outboxMessageRepository).markPublished(eq(1L), any(LocalDateTime.class));
    }
}
