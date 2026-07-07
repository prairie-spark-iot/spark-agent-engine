package com.spark.agent.kafka;

import com.spark.agent.entity.DeviceData;
import com.spark.agent.ws.WsPushService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Independent groupId="ws-bridge" consumer on iot.device.data — a second consumer group on the
 * same topic OutboxRelayService already publishes to, so this has zero effect on offsets or
 * behavior of any existing consumer. Coalesces per-property updates into one snapshot per device
 * per flush tick, since one MQTT telemetry message fans out into N Kafka records (one per
 * property) via TelemetryService/OutboxMessageFactory. See
 * docs/superpowers/specs/2026-07-06-websocket-realtime-design.md.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryWsBridgeConsumer {

    private final ObjectMapper objectMapper;
    private final WsPushService wsPushService;

    private final Map<String, DeviceSnapshot> pending = new ConcurrentHashMap<>();

    @KafkaListener(topics = "${kafka.topic.device-data:iot.device.data}", groupId = "ws-bridge")
    public void onDeviceData(ConsumerRecord<String, String> record) {
        DeviceData row;
        try {
            row = objectMapper.readValue(record.value(), DeviceData.class);
        } catch (Exception e) {
            log.error("[WsBridge] Failed to parse device data message: {}", e.getMessage());
            return;
        }
        pending.computeIfAbsent(row.getDeviceKey(), k -> new DeviceSnapshot())
                .merge(row.getIdentifier(), row.getValueNum() != null ? row.getValueNum() : row.getValue(), row.getReportTime());
    }

    @Scheduled(fixedDelayString = "${app.telemetry-ws-coalesce-interval-ms:400}")
    public void flush() {
        Set<String> deviceKeys = Set.copyOf(pending.keySet());
        for (String deviceKey : deviceKeys) {
            DeviceSnapshot snapshot = pending.remove(deviceKey);
            if (snapshot != null) {
                wsPushService.pushTelemetry(deviceKey, snapshot.toPayload(deviceKey));
            }
        }
    }

    private static class DeviceSnapshot {
        private final Map<String, Object> properties = new ConcurrentHashMap<>();
        private volatile LocalDateTime latestReportTime;

        void merge(String identifier, Object value, LocalDateTime reportTime) {
            properties.put(identifier, value);
            if (latestReportTime == null || (reportTime != null && reportTime.isAfter(latestReportTime))) {
                latestReportTime = reportTime;
            }
        }

        TelemetrySnapshot toPayload(String deviceKey) {
            return new TelemetrySnapshot(deviceKey, Map.copyOf(properties), latestReportTime);
        }
    }

    public record TelemetrySnapshot(String deviceKey, Map<String, Object> properties, LocalDateTime reportTime) {
    }
}
