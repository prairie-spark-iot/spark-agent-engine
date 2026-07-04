package com.spark.agent.mqtt;

import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.spark.agent.config.MqttProperties;
import com.spark.agent.entity.Device;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.service.DeviceHeartbeatService;
import com.spark.agent.service.TelemetryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttSubscriberTest {

    @Mock private TelemetryService telemetryService;
    @Mock private DeviceRepository deviceRepository;
    @Mock private DeviceHeartbeatService heartbeatService;
    @Mock private Mqtt5Publish publish;
    @Mock private MqttTopic topic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttSubscriber subscriber;

    @BeforeEach
    void setUp() {
        subscriber = new MqttSubscriber(new MqttProperties(), telemetryService, deviceRepository, heartbeatService, objectMapper);
    }

    @Test
    void dispatch_validJson_deserializesAndInvokesTelemetryService() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"productKey\":\"PK_TEST\",\"timestamp\":1719655200000,\"properties\":{\"temperature\":235.5}}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        subscriber.dispatch(publish);

        verify(telemetryService).process(argThat(msg ->
                "DK_TEST_001".equals(msg.getDeviceKey()) && "PK_TEST".equals(msg.getProductKey())));
    }

    @Test
    void dispatch_invalidJson_doesNotThrowAndSkipsTelemetryService() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatch(publish));

        verifyNoInteractions(telemetryService);
    }

    @Test
    void dispatch_telemetryServiceThrows_exceptionIsCaughtNotPropagated() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"productKey\":\"PK_TEST\",\"timestamp\":1719655200000,\"properties\":{\"temperature\":235.5}}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);
        doThrow(new RuntimeException("db down")).when(telemetryService).process(any());

        assertDoesNotThrow(() -> subscriber.dispatch(publish));
    }

    @Test
    void dispatchOnline_knownDevice_callsHeartbeat() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"status\":\"online\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        Device device = new Device();
        device.setId(42L);
        device.setDeviceKey("DK_TEST_001");
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0))
                .thenReturn(Optional.of(device));

        subscriber.dispatchOnline(publish);

        verify(heartbeatService).heartbeat(42L, "DK_TEST_001");
    }

    @Test
    void dispatchOnline_unknownDevice_doesNotCallHeartbeat() {
        String json = "{\"deviceKey\":\"DK_UNKNOWN\",\"status\":\"online\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_UNKNOWN", (short) 0))
                .thenReturn(Optional.empty());

        subscriber.dispatchOnline(publish);

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchOnline_invalidJson_doesNotThrow() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatchOnline(publish));

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchOffline_realDeviceKey_callsMarkOfflineNow() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"status\":\"offline\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        subscriber.dispatchOffline(publish);

        verify(heartbeatService).markOfflineNow("DK_TEST_001");
        verify(heartbeatService, never()).markAllOffline();
    }

    @Test
    void dispatchOffline_emulatorDeviceKey_callsMarkAllOffline() {
        String json = "{\"deviceKey\":\"emulator\",\"status\":\"offline\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        subscriber.dispatchOffline(publish);

        verify(heartbeatService).markAllOffline();
        verify(heartbeatService, never()).markOfflineNow(any());
    }

    @Test
    void dispatchOffline_invalidJson_doesNotThrow() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatchOffline(publish));

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchStatus_knownDevice_callsHeartbeat() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"productKey\":\"PK_TEST\",\"timestamp\":1719655200000,\"status\":\"running\",\"uptime\":120}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        Device device = new Device();
        device.setId(42L);
        device.setDeviceKey("DK_TEST_001");
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0))
                .thenReturn(Optional.of(device));

        subscriber.dispatchStatus(publish);

        verify(heartbeatService).heartbeat(42L, "DK_TEST_001");
    }

    @Test
    void dispatchStatus_unknownDevice_doesNotCallHeartbeat() {
        String json = "{\"deviceKey\":\"DK_UNKNOWN\",\"status\":\"running\",\"uptime\":120}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_UNKNOWN", (short) 0))
                .thenReturn(Optional.empty());

        subscriber.dispatchStatus(publish);

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchStatus_invalidJson_doesNotThrow() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatchStatus(publish));

        verifyNoInteractions(heartbeatService);
    }
}
