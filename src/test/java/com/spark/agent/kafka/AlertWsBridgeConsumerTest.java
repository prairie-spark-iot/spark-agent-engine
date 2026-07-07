package com.spark.agent.kafka;

import com.spark.agent.entity.AlertRecord;
import com.spark.agent.ws.WsPushService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class AlertWsBridgeConsumerTest {

    @Mock
    private WsPushService wsPushService;

    private AlertWsBridgeConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AlertWsBridgeConsumer(new ObjectMapper(), wsPushService);
    }

    @Test
    void onAlertTriggered_validPayload_pushesAlertRecord() {
        String json = """
                {"id":100,"deviceKey":"DK_TEST_001","identifier":"temperature","level":2}
                """.strip();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("iot.alert.triggered", 0, 0L, "DK_TEST_001", json);

        consumer.onAlertTriggered(record);

        verify(wsPushService).pushAlert(argThat((AlertRecord alert) -> alert.getId() == 100L
                && "DK_TEST_001".equals(alert.getDeviceKey())));
    }

    @Test
    void onAlertTriggered_invalidJson_doesNotThrowAndSkipsPush() {
        ConsumerRecord<String, String> malformed = new ConsumerRecord<>("iot.alert.triggered", 0, 0L, "DK_TEST_001", "not json");

        assertDoesNotThrow(() -> consumer.onAlertTriggered(malformed));

        verifyNoInteractions(wsPushService);
    }
}
