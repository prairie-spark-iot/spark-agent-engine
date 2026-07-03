package com.spark.agent.mqtt;

import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.spark.agent.config.MqttProperties;
import com.spark.agent.service.TelemetryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttSubscriberTest {

    @Mock private TelemetryService telemetryService;
    @Mock private Mqtt5Publish publish;
    @Mock private MqttTopic topic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttSubscriber subscriber;

    @BeforeEach
    void setUp() {
        subscriber = new MqttSubscriber(new MqttProperties(), telemetryService, objectMapper);
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
}
