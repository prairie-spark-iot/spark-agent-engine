package com.spark.agent.service;

import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.repository.VectorStoreRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds LLM prompts and formats diagnosis context data.
 * Extracted from {@link DiagnosisAgentService} to keep orchestration clean.
 */
@Component
public class DiagnosisPromptBuilder {

    private static final String SYSTEM_PROMPT = """
            You are an expert industrial IoT diagnosis assistant for factory equipment.
            Given an alert, the device's recent telemetry, its alert history, and excerpts
            from equipment manuals, determine the most likely root cause and a concrete,
            actionable remediation. Be specific and concise.
            """;

    private static final String REFLECTION_INSTRUCTION =
            "\n## Reflection\nYour previous analysis had low confidence or lacked manual references. " +
            "Re-analyze with deeper causal relationships using the widened telemetry window above.\n";

    private static final int MAX_EXCERPT_LENGTH = 4000;

    public String getSystemPrompt() {
        return SYSTEM_PROMPT;
    }

    /**
     * Build a complete user prompt for the LLM.
     *
     * @param alert          the alert being diagnosed
     * @param device         the device that triggered the alert (nullable)
     * @param telemetryTrend formatted telemetry trend string
     * @param alertHistory   formatted alert history string
     * @param manualExcerpts formatted manual excerpts string
     * @param reflection     whether this is a reflection retry
     */
    public String buildUserPrompt(AlertRecord alert, Device device, String telemetryTrend,
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

    /**
     * Format telemetry data for inclusion in the prompt.
     */
    public String formatTelemetry(List<DeviceData> telemetry, String windowLabel) {
        String trend = telemetry.stream()
                .map(d -> "%s=%s @ %s".formatted(
                        d.getIdentifier(),
                        d.getValue() != null ? d.getValue() : "null",
                        d.getReportTime()))
                .collect(Collectors.joining("\n"));
        return trend.isBlank() ? "no data (%s)".formatted(windowLabel) : trend;
    }

    /**
     * Format RAG manual search results for inclusion in the prompt.
     */
    public String formatManuals(List<VectorStoreRepository.SearchResult> manuals) {
        String excerpts = manuals.stream()
                .map(m -> "[%s] %s".formatted(m.title(), m.chunkText()))
                .collect(Collectors.joining("\n---\n"));
        if (excerpts.isBlank()) return "none found";
        return excerpts.length() <= MAX_EXCERPT_LENGTH
                ? excerpts
                : excerpts.substring(0, MAX_EXCERPT_LENGTH) + "\n---\n[truncated]";
    }
}
