package com.spark.agent.ws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.LocalDateTime;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class WsPushServiceTest {

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    private WsPushService service;

    @BeforeEach
    void setUp() {
        service = new WsPushService(messagingTemplate);
    }

    @Test
    void pushTelemetry_sendsToPerDeviceTopic() {
        Object snapshot = new Object();

        service.pushTelemetry("DK_TEST_001", snapshot);

        verify(messagingTemplate).convertAndSend("/topic/telemetry/DK_TEST_001", snapshot);
    }

    @Test
    void pushAlert_sendsToSharedAlertsTopic() {
        Object alert = new Object();

        service.pushAlert(alert);

        verify(messagingTemplate).convertAndSend("/topic/alerts", alert);
    }

    @Test
    void pushDiagnosis_sendsToPerAlertTopic() {
        Object alert = new Object();

        service.pushDiagnosis(42L, alert);

        verify(messagingTemplate).convertAndSend("/topic/diagnosis/42", alert);
    }

    @Test
    void pushDeviceStatus_sendsToSharedStatusTopic() {
        DeviceStatusEvent event = new DeviceStatusEvent("DK_TEST_001", true, LocalDateTime.now());

        service.pushDeviceStatus(event);

        verify(messagingTemplate).convertAndSend("/topic/devices/status", event);
    }
}
