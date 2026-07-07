package com.spark.agent.kafka;

import com.spark.agent.ws.WsPushService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class TelemetryWsBridgeConsumerTest {

    @Mock
    private WsPushService wsPushService;

    private TelemetryWsBridgeConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new TelemetryWsBridgeConsumer(new ObjectMapper(), wsPushService);
    }

    private ConsumerRecord<String, String> deviceDataRecord(String deviceKey, String identifier, double valueNum, String reportTime) {
        String json = """
                {"deviceKey":"%s","identifier":"%s","value":"%s","valueNum":%s,"quality":1,"reportTime":"%s"}
                """.formatted(deviceKey, identifier, valueNum, valueNum, reportTime).strip();
        return new ConsumerRecord<>("iot.device.data", 0, 0L, deviceKey, json);
    }

    @Test
    void onDeviceData_thenFlush_sendsCoalescedSnapshotPerDevice() {
        consumer.onDeviceData(deviceDataRecord("DK_TEST_001", "temperature", 235.5, "2026-07-06T10:00:00"));
        consumer.onDeviceData(deviceDataRecord("DK_TEST_001", "pressure", 156.2, "2026-07-06T10:00:01"));

        consumer.flush();

        ArgumentCaptor<TelemetryWsBridgeConsumer.TelemetrySnapshot> captor =
                ArgumentCaptor.forClass(TelemetryWsBridgeConsumer.TelemetrySnapshot.class);
        verify(wsPushService).pushTelemetry(org.mockito.ArgumentMatchers.eq("DK_TEST_001"), captor.capture());

        TelemetryWsBridgeConsumer.TelemetrySnapshot snapshot = captor.getValue();
        assertEquals("DK_TEST_001", snapshot.deviceKey());
        assertEquals(2, snapshot.properties().size());
        assertNotNull(snapshot.properties().get("temperature"));
        assertNotNull(snapshot.properties().get("pressure"));
    }

    @Test
    void flush_noPendingUpdates_doesNotPush() {
        consumer.flush();

        verifyNoInteractions(wsPushService);
    }

    @Test
    void onDeviceData_invalidJson_doesNotThrowAndSkips() {
        ConsumerRecord<String, String> malformed = new ConsumerRecord<>("iot.device.data", 0, 0L, "DK_TEST_001", "not json");

        assertDoesNotThrow(() -> consumer.onDeviceData(malformed));
        consumer.flush();

        verifyNoInteractions(wsPushService);
    }

    @Test
    void flush_multipleDevices_pushesOncePerDeviceToItsOwnTopic() {
        consumer.onDeviceData(deviceDataRecord("DK_TEST_A", "temperature", 100.0, "2026-07-06T10:00:00"));
        consumer.onDeviceData(deviceDataRecord("DK_TEST_B", "temperature", 200.0, "2026-07-06T10:00:00"));

        consumer.flush();

        verify(wsPushService).pushTelemetry(org.mockito.ArgumentMatchers.eq("DK_TEST_A"), org.mockito.ArgumentMatchers.any());
        verify(wsPushService).pushTelemetry(org.mockito.ArgumentMatchers.eq("DK_TEST_B"), org.mockito.ArgumentMatchers.any());
    }
}
