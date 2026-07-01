package com.spark.agent.service;

import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.VectorStoreRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

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

    private final DeviceRepository deviceRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final RagSearchService ragSearchService;
    private final ChatClient.Builder chatClientBuilder;
    private final ObjectMapper objectMapper;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    public DiagnosisResult diagnose(String alertMessage) {
        AlertRecord alert;
        try {
            alert = objectMapper.readValue(alertMessage, AlertRecord.class);
        } catch (Exception e) {
            log.error("[Diagnosis] Failed to parse alert message: {}", e.getMessage());
            return null;
        }

        Device device = deviceRepository.findByDeviceKeyAndDeleted(alert.getDeviceKey(), (short) 0)
                .orElse(null);

        List<DeviceData> latestTelemetry = deviceDataRepository.findLatestByDeviceKey(alert.getDeviceKey());
        String telemetryTrend = latestTelemetry.stream()
                .map(d -> "%s=%s @ %s".formatted(d.getIdentifier(), d.getValue(), d.getReportTime()))
                .collect(Collectors.joining("\n"));

        List<AlertRecord> pastAlerts = alertRecordRepository
                .findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(alert.getDeviceKey(), (short) 0);
        String alertHistory = pastAlerts.stream()
                .map(a -> "[%s] %s (level=%d) @ %s".formatted(a.getIdentifier(), a.getAlertContent(), a.getLevel(), a.getTriggerTime()))
                .collect(Collectors.joining("\n"));

        // Device has no explicit "model" field; deviceName is the closest descriptive
        // string and is what knowledge docs are tagged with at ingestion time.
        String deviceModel = device != null ? device.getDeviceName() : null;
        List<VectorStoreRepository.SearchResult> manuals =
                ragSearchService.search(alert.getAlertContent(), deviceModel, 3);
        String manualExcerpts = manuals.stream()
                .map(m -> "[%s] %s".formatted(m.title(), m.chunkText()))
                .collect(Collectors.joining("\n---\n"));

        String userPrompt = """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                ## Device Info
                %s

                ## Recent Telemetry (latest per identifier)
                %s

                ## Recent Alert History (same device)
                %s

                ## Relevant Manual Excerpts
                %s
                """.formatted(
                alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(), alert.getLevel(),
                alert.getAlertContent(), alert.getTriggerTime(),
                device != null ? "%s (key=%s)".formatted(device.getDeviceName(), device.getDeviceKey()) : "unknown",
                telemetryTrend.isBlank() ? "no data" : telemetryTrend,
                alertHistory.isBlank() ? "none" : alertHistory,
                manualExcerpts.isBlank() ? "none found" : manualExcerpts);

        return chatClient.prompt()
                .system(SYSTEM_PROMPT)
                .user(userPrompt)
                .call()
                .entity(DiagnosisResult.class);
    }
}
