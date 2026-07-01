package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.repository.VectorStoreRepository;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisAgentService {

    /** diagnosis_status values written back to aiot_alert_record */
    private static final short STATUS_HUMAN_REVIEW_REQUIRED = 1;
    private static final short STATUS_DIAGNOSED = 2;

    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final RagSearchService ragSearchService;
    private final ChatClient.Builder chatClientBuilder;
    private final AppProperties appProperties;
    private final DiagnosisPromptBuilder promptBuilder;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    /**
     * Run AI diagnosis for the given alert. Fetches the managed entity from DB,
     * gathers context (device, product, telemetry, past alerts, RAG manuals),
     * invokes the LLM, optionally retries with reflection, and writes back the result.
     *
     * @param alertId the {@code aiot_alert_record.id} to diagnose
     * @return the diagnosis result, or null if the alert record no longer exists
     */
    @Transactional
    public DiagnosisResult diagnose(Long alertId) {
        AlertRecord alert = alertRecordRepository.findById(alertId)
                .orElseThrow(() -> new EntityNotFoundException(
                        "AlertRecord " + alertId + " not found for diagnosis"));

        Device device = deviceRepository.findByDeviceKeyAndDeleted(alert.getDeviceKey(), (short) 0)
                .orElse(null);

        String deviceModel = resolveDeviceModel(device);

        String alertHistory = formatAlertHistory(alert.getDeviceKey());

        List<VectorStoreRepository.SearchResult> manuals =
                ragSearchService.search(alert.getAlertContent(), deviceModel, 3);
        String manualExcerpts = promptBuilder.formatManuals(manuals);

        List<DeviceData> latestTelemetry = deviceDataRepository.findLatestByDeviceKey(alert.getDeviceKey());
        String telemetryTrend = promptBuilder.formatTelemetry(latestTelemetry, "latest per identifier");

        String initialPrompt = promptBuilder.buildUserPrompt(alert, device, telemetryTrend, alertHistory, manualExcerpts, false);
        DiagnosisResult result = runInference(initialPrompt);

        if (needsReflection(result, manuals)) {
            result = runReflection(alert, device, alertHistory, manualExcerpts, result);
        }

        writeBack(alert, result);
        return result;
    }

    private String resolveDeviceModel(Device device) {
        if (device == null) return null;
        return productRepository.findById(device.getProductId())
                .map(Product::getProductKey)
                .orElse(null);
    }

    private String formatAlertHistory(String deviceKey) {
        List<AlertRecord> pastAlerts = alertRecordRepository
                .findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(deviceKey, (short) 0);
        return pastAlerts.stream()
                .map(a -> "[%s] %s (level=%d) @ %s".formatted(
                        a.getIdentifier(), a.getAlertContent(), a.getLevel(), a.getTriggerTime()))
                .collect(Collectors.joining("\n"));
    }

    private DiagnosisResult runReflection(AlertRecord alert, Device device,
                                           String alertHistory, String manualExcerpts,
                                           DiagnosisResult initialResult) {
        log.warn("[Diagnosis][Reflection] alert={} device={} confidence={} manualsFound={} — " +
                        "retrying once with a widened telemetry window and a deeper-analysis prompt",
                alert.getId(), alert.getDeviceKey(), initialResult.confidence(),
                manualExcerpts.isEmpty() || "none found".equals(manualExcerpts) ? 0 : 1);

        LocalDateTime since = LocalDateTime.now()
                .minusMinutes(appProperties.getDiagnosisReflectionTelemetryWindowMinutes());
        List<DeviceData> widenedTelemetry = deviceDataRepository
                .findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                        alert.getDeviceKey(), (short) 0, since, PageRequest.of(0, 100));
        String widenedTrend = promptBuilder.formatTelemetry(widenedTelemetry,
                "last %d minutes".formatted(appProperties.getDiagnosisReflectionTelemetryWindowMinutes()));

        String reflectionPrompt = promptBuilder.buildUserPrompt(
                alert, device, widenedTrend, alertHistory, manualExcerpts, true);
        DiagnosisResult reflected = runInference(reflectionPrompt);

        return new DiagnosisResult(
                reflected.rootCause(), reflected.suggestion(), reflected.confidence(),
                """
                === Initial Analysis (confidence=%d) ===
                %s

                === Reflection Retry (confidence=%d) ===
                %s
                """.formatted(initialResult.confidence(), initialResult.diagnosisDetail(),
                        reflected.confidence(), reflected.diagnosisDetail()));
    }

    private DiagnosisResult runInference(String userPrompt) {
        try {
            return CompletableFuture.supplyAsync(() ->
                    chatClient.prompt()
                            .system(promptBuilder.getSystemPrompt())
                            .user(userPrompt)
                            .call()
                            .entity(DiagnosisResult.class)
            ).orTimeout(60, TimeUnit.SECONDS).join();
        } catch (Exception e) {
            log.error("[Diagnosis] LLM inference failed or timed out: {}", e.getMessage());
            return new DiagnosisResult("", "", 0, "Inference failed: " + e.getMessage());
        }
    }

    private boolean needsReflection(DiagnosisResult result,
                                     List<VectorStoreRepository.SearchResult> manuals) {
        return result.confidence() < appProperties.getDiagnosisReflectionConfidenceThreshold()
                || manuals.isEmpty();
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
}
