package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.repository.AlertRecordRepository;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisAgentService {

    /** diagnosis_status values written back to aiot_alert_record */
    private static final short STATUS_HUMAN_REVIEW_REQUIRED = 1;
    private static final short STATUS_DIAGNOSED = 2;

    private final AlertRecordRepository alertRecordRepository;
    private final ChatClient.Builder chatClientBuilder;
    private final ToolCallbackProvider deviceToolCallbacks;
    private final AppProperties appProperties;
    private final DiagnosisPromptBuilder promptBuilder;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    /**
     * Run AI diagnosis for the given alert. The LLM decides for itself which device
     * tools (if any) to call before producing a final diagnosis, then the result is
     * written back to the alert record.
     *
     * @param alertId the {@code aiot_alert_record.id} to diagnose
     * @return the diagnosis result
     */
    @Transactional
    public DiagnosisResult diagnose(Long alertId) {
        AlertRecord alert = alertRecordRepository.findById(alertId)
                .orElseThrow(() -> new EntityNotFoundException(
                        "AlertRecord " + alertId + " not found for diagnosis"));

        DiagnosisResult result = runInference(promptBuilder.userPrompt(alert));

        if (result.confidence() < appProperties.getDiagnosisRetryConfidenceThreshold()) {
            DiagnosisResult retryResult = runInference(promptBuilder.retryUserPrompt(alert));
            if (retryResult.confidence() > result.confidence()) {
                result = retryResult;
            }
        }

        writeBack(alert, result);
        return result;
    }

    private DiagnosisResult runInference(String userPrompt) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                String investigation = chatClient.prompt()
                        .system(promptBuilder.systemPrompt())
                        .tools(deviceToolCallbacks)
                        .user(userPrompt)
                        .call()
                        .content();
                return chatClient.prompt()
                        .system(promptBuilder.structureSystemPrompt())
                        .user(investigation)
                        .call()
                        .entity(DiagnosisResult.class);
            }).orTimeout(150, TimeUnit.SECONDS).join();
        } catch (Exception e) {
            log.error("[Diagnosis] LLM inference failed or timed out: {}", e.getMessage());
            return new DiagnosisResult("", "", 0, "Inference failed: " + e.getMessage());
        }
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
