package com.spark.agent.kafka;

import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.DeviceData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.device-data:iot.device.data}")
    private String deviceDataTopic;

    @Value("${kafka.topic.alert-triggered:iot.alert.triggered}")
    private String alertTopic;

    public void sendTelemetry(DeviceData data) {
        send(deviceDataTopic, data.getDeviceKey(), data);
    }

    public void sendAlert(AlertRecord record) {
        send(alertTopic, record.getDeviceKey(), record);
    }

    public CompletableFuture<SendResult<Object, Object>> sendRaw(String topic, String key, String jsonPayload) {
        return kafkaTemplate.send(topic, key, jsonPayload);
    }

    private void send(String topic, String key, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json)
                    .thenAccept(r -> log.debug("[Kafka] Sent to {} key={}", topic, key))
                    .exceptionally(ex -> {
                        log.error("[Kafka] Failed to send to {}: {}", topic, ex.getMessage());
                        return null;
                    });
        } catch (Exception e) {
            log.error("[Kafka] Serialization failed for topic {}: {}", topic, e.getMessage());
        }
    }
}
