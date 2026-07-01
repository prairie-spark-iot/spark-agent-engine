package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.VectorStoreRepository;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisAgentService {

    private static final String SYSTEM_PROMPT = """
            You are an expert industrial IoT diagnosis assistant for factory equipment.
            Given an alert, the device's recent telemetry, its alert history, and excerpts
            from equipment manuals, determine the most likely root cause and a concrete,
            actionable remediation. Be specific and concise.
            """;

    private static final String REFLECTION_INSTRUCTION =
            "\n## Reflection\nYour previous analysis had low confidence or lacked manual references. " +
            "Re-analyze with deeper causal relationships using the widened telemetry window above.\n";

    /** diagnosis_status values written back to aiot_alert_record */
    private static final short STATUS_HUMAN_REVIEW_REQUIRED = 1;
    private static final short STATUS_DIAGNOSED = 2;

    private final DeviceRepository deviceRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final RagSearchService ragSearchService;
    private final ChatClient.Builder chatClientBuilder;
    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    @Transactional
    public DiagnosisResult diagnose(String alertMessage) {
        AlertRecord kafkaAlert;
        try {
            kafkaAlert = objectMapper.readValue(alertMessage, AlertRecord.class);
        } catch (Exception e) {
            log.error("[Diagnosis] Failed to parse alert message: {}", e.getMessage());
            return null;
        }

        // The Kafka payload is a detached, possibly-incomplete snapshot (e.g. deleted defaults to 0
        // via Jackson but any field the producer omitted comes back null). Re-fetch the managed row
        // from Postgres and do all reads/writes against that instead of the wire object.
        AlertRecord alert = alertRecordRepository.findById(kafkaAlert.getId())
                .orElseThrow(() -> new EntityNotFoundException(
                        "AlertRecord " + kafkaAlert.getId() + " not found for diagnosis"));

        Device device = deviceRepository.findByDeviceKeyAndDeleted(alert.getDeviceKey(), (short) 0)
                .orElse(null);
        // Device has no explicit "model" field; deviceName is the closest descriptive
        // string and is what knowledge docs are tagged with at ingestion time.
        String deviceModel = device != null ? device.getDeviceName() : null;

        List<AlertRecord> pastAlerts = alertRecordRepository
                .findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(alert.getDeviceKey(), (short) 0);
        String alertHistory = pastAlerts.stream()
                .map(a -> "[%s] %s (level=%d) @ %s".formatted(a.getIdentifier(), a.getAlertContent(), a.getLevel(), a.getTriggerTime()))
                .collect(Collectors.joining("\n"));

        List<VectorStoreRepository.SearchResult> manuals =
                ragSearchService.search(alert.getAlertContent(), deviceModel, 3);
        String manualExcerpts = formatManuals(manuals);

        List<DeviceData> latestTelemetry = deviceDataRepository.findLatestByDeviceKey(alert.getDeviceKey());
        String telemetryTrend = formatTelemetry(latestTelemetry, "latest per identifier");

        String initialPrompt = buildUserPrompt(alert, device, telemetryTrend, alertHistory, manualExcerpts, false);
        DiagnosisResult result = runInference(initialPrompt);

        if (needsReflection(result, manuals)) {
            log.warn("[Diagnosis][Reflection] alert={} device={} confidence={} manualsFound={} — " +
                            "retrying once with a widened telemetry window and a deeper-analysis prompt",
                    alert.getId(), alert.getDeviceKey(), result.confidence(), manuals.size());

            LocalDateTime since = LocalDateTime.now().minusMinutes(appProperties.getDiagnosisReflectionTelemetryWindowMinutes());
            List<DeviceData> widenedTelemetry = deviceDataRepository
                    .findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                            alert.getDeviceKey(), (short) 0, since, PageRequest.of(0, 100));
            String widenedTrend = formatTelemetry(widenedTelemetry,
                    "last %d minutes".formatted(appProperties.getDiagnosisReflectionTelemetryWindowMinutes()));

            String reflectionPrompt = buildUserPrompt(alert, device, widenedTrend, alertHistory, manualExcerpts, true);
            DiagnosisResult reflected = runInference(reflectionPrompt);

            result = new DiagnosisResult(
                    reflected.rootCause(), reflected.suggestion(), reflected.confidence(),
                    """
                    === Initial Analysis (confidence=%d) ===
                    %s

                    === Reflection Retry (confidence=%d) ===
                    %s
                    """.formatted(result.confidence(), result.diagnosisDetail(),
                            reflected.confidence(), reflected.diagnosisDetail()));
        }

        writeBack(alert, result);
        return result;
    }

    private DiagnosisResult runInference(String userPrompt) {
        return chatClient.prompt()
                .system(SYSTEM_PROMPT)
                .user(userPrompt)
                .call()
                .entity(DiagnosisResult.class);
    }

    private boolean needsReflection(DiagnosisResult result, List<VectorStoreRepository.SearchResult> manuals) {
        return result.confidence() < appProperties.getDiagnosisReflectionConfidenceThreshold() || manuals.isEmpty();
    }

    private void writeBack(AlertRecord record, DiagnosisResult result) {
        boolean autoDiagnosed = result.confidence() >= appProperties.getDiagnosisConfidenceThreshold();
        record.setRootCause(result.rootCause());
        record.setSuggestion(result.suggestion());
        record.setConfidence(BigDecimal.valueOf(result.confidence()));
        record.setDiagnosisDetail(result.diagnosisDetail());
        record.setDiagnosisStatus(autoDiagnosed ? STATUS_DIAGNOSED : STATUS_HUMAN_REVIEW_REQUIRED);
        record.setDiagnosisTime(LocalDateTime.now());
        record.setDeleted((short) 0); // a diagnosis writeback must never leave the record logically deleted
        alertRecordRepository.save(record);

        log.info("[Diagnosis] alert={} status={} confidence={}", record.getId(),
                autoDiagnosed ? "AUTO_DIAGNOSED" : "HUMAN_REVIEW_REQUIRED", result.confidence());
    }

    private String formatTelemetry(List<DeviceData> telemetry, String windowLabel) {
        String trend = telemetry.stream()
                .map(d -> "%s=%s @ %s".formatted(d.getIdentifier(), d.getValue(), d.getReportTime()))
                .collect(Collectors.joining("\n"));
        return trend.isBlank() ? "no data (%s)".formatted(windowLabel) : trend;
    }

    private String formatManuals(List<VectorStoreRepository.SearchResult> manuals) {
        String excerpts = manuals.stream()
                .map(m -> "[%s] %s".formatted(m.title(), m.chunkText()))
                .collect(Collectors.joining("\n---\n"));
        return excerpts.isBlank() ? "none found" : excerpts;
    }

    private String buildUserPrompt(AlertRecord alert, Device device, String telemetryTrend,
                                    String alertHistory, String manualExcerpts, boolean reflection) {
        return """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                ## Device Info
                %s

                ## Recent Telemetry (%s)
                %s

                ## Recent Alert History (same device)
                %s

                ## Relevant Manual Excerpts
                %s
                %s""".formatted(
                alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(), alert.getLevel(),
                alert.getAlertContent(), alert.getTriggerTime(),
                device != null ? "%s (key=%s)".formatted(device.getDeviceName(), device.getDeviceKey()) : "unknown",
                reflection ? "widened window" : "latest per identifier",
                telemetryTrend,
                alertHistory.isBlank() ? "none" : alertHistory,
                manualExcerpts,
                reflection ? REFLECTION_INSTRUCTION : "");
    }
}
