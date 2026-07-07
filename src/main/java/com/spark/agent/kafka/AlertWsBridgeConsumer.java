package com.spark.agent.kafka;

import com.spark.agent.entity.AlertRecord;
import com.spark.agent.ws.WsPushService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Independent groupId="ws-bridge" consumer on iot.alert.triggered — a second consumer group on
 * the same topic AlertTriggeredConsumer (groupId="diagnosis-agent") already consumes, so this has
 * zero effect on that consumer's offsets, retries, or the diagnosis pipeline. See
 * docs/superpowers/specs/2026-07-06-websocket-realtime-design.md.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertWsBridgeConsumer {

    private final ObjectMapper objectMapper;
    private final WsPushService wsPushService;

    @KafkaListener(topics = "${kafka.topic.alert-triggered:iot.alert.triggered}", groupId = "ws-bridge")
    public void onAlertTriggered(ConsumerRecord<String, String> record) {
        AlertRecord alert;
        try {
            alert = objectMapper.readValue(record.value(), AlertRecord.class);
        } catch (Exception e) {
            log.error("[WsBridge] Failed to parse alert message: {}", e.getMessage());
            return;
        }
        wsPushService.pushAlert(alert);
    }
}
