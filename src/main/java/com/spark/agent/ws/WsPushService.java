package com.spark.agent.ws;

import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class WsPushService {

    private final SimpMessagingTemplate messagingTemplate;

    public void pushTelemetry(String deviceKey, Object snapshot) {
        messagingTemplate.convertAndSend("/topic/telemetry/" + deviceKey, snapshot);
    }

    public void pushAlert(Object alertRecord) {
        messagingTemplate.convertAndSend("/topic/alerts", alertRecord);
    }

    public void pushDiagnosis(Long alertId, Object alertRecord) {
        messagingTemplate.convertAndSend("/topic/diagnosis/" + alertId, alertRecord);
    }

    public void pushDeviceStatus(DeviceStatusEvent event) {
        messagingTemplate.convertAndSend("/topic/devices/status", event);
    }
}
