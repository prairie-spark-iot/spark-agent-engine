package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxRelayService {

    private static final Map<String, String> EVENT_TYPE_TOPICS = Map.of(
            "device.data", "iot.device.data",
            "alert.triggered", "iot.alert.triggered"
    );

    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;

    @Scheduled(fixedDelayString = "${app.outbox-relay-interval-ms:2000}")
    public void relay() {
        List<OutboxMessage> batch = outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(
                PageRequest.of(0, appProperties.getOutboxRelayBatchSize()));

        for (OutboxMessage msg : batch) {
            try {
                String topic = EVENT_TYPE_TOPICS.get(msg.getEventType());
                if (topic == null) {
                    log.error("[OutboxRelay] Unknown eventType '{}' for outbox id={}, skipping", msg.getEventType(), msg.getId());
                    continue;
                }
                String key = extractDeviceKey(msg.getPayload());
                kafkaProducerService.sendRaw(topic, key, msg.getPayload()).get(5, TimeUnit.SECONDS);
                outboxMessageRepository.markPublished(msg.getId(), LocalDateTime.now());
            } catch (Exception e) {
                log.warn("[OutboxRelay] Failed to publish outbox id={}, will retry next cycle: {}", msg.getId(), e.getMessage());
            }
        }
    }

    private String extractDeviceKey(String payloadJson) {
        return objectMapper.readTree(payloadJson).get("deviceKey").asText();
    }

    @Scheduled(cron = "${app.outbox-purge-cron:0 0 3 * * *}")
    public void purge() {
        LocalDateTime cutoff = LocalDateTime.now().minusDays(appProperties.getOutboxPurgeRetentionDays());
        int deleted = outboxMessageRepository.deletePublishedBefore(cutoff);
        if (deleted > 0) {
            log.info("[OutboxRelay] Purged {} published outbox rows older than {}", deleted, cutoff);
        }
    }
}
