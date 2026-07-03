package com.spark.agent.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    private KafkaProducerService service;

    @BeforeEach
    void setUp() {
        service = new KafkaProducerService(kafkaTemplate);
    }

    @Test
    void sendRaw_sendsExactPayloadWithNoReSerialization() {
        CompletableFuture<org.springframework.kafka.support.SendResult<Object, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send("iot.device.data", "DK_TEST_001", "{\"deviceKey\":\"DK_TEST_001\"}"))
                .thenReturn(future);

        var result = service.sendRaw("iot.device.data", "DK_TEST_001", "{\"deviceKey\":\"DK_TEST_001\"}");

        assertSame(future, result);
        verify(kafkaTemplate).send("iot.device.data", "DK_TEST_001", "{\"deviceKey\":\"DK_TEST_001\"}");
    }
}
