package com.spark.agent.kafka;

import com.spark.agent.entity.AlertRecord;
import com.spark.agent.service.DiagnosisAgentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertTriggeredConsumer {

    private final ObjectMapper objectMapper;
    private final DiagnosisAgentService diagnosisAgentService;

    @KafkaListener(topics = "${kafka.topic.alert-triggered:iot.alert.triggered}", groupId = "diagnosis-agent")
    public void onAlertTriggered(ConsumerRecord<String, String> record) {
        String payload = record.value();
        Long alertId;
        try {
            AlertRecord alert = objectMapper.readValue(payload, AlertRecord.class);
            alertId = alert.getId();
            log.info("[Diagnosis] Alert triggered device={} identifier={} level={}",
                    alert.getDeviceKey(), alert.getIdentifier(), alert.getLevel());
        } catch (Exception e) {
            log.error("[Diagnosis] Failed to parse alert message: {}", e.getMessage());
            return;
        }
        diagnosisAgentService.diagnose(alertId);
    }
}
