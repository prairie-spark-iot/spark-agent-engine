package com.spark.agent.service;

import com.spark.agent.entity.AlertRecord;
import org.springframework.stereotype.Component;

@Component
public class DiagnosisPromptBuilder {

    private static final String SYSTEM_PROMPT = """
            You are an expert industrial IoT diagnosis assistant for factory equipment.
            You have tools available to investigate an alert: queryDeviceStatus (device info
            and latest telemetry), queryDeviceHistory (historical telemetry for one
            identifier), queryDeviceAlerts (past alerts for the device), and queryDeviceManual
            (search equipment manuals by device model and question/symptom).
            Use these tools as needed to gather the context you need — call queryDeviceStatus
            first if you need the device's product model to search its manual. Then determine
            the most likely root cause and a concrete, actionable remediation. Be specific and
            concise.
            """;

    /**
     * Structured output is requested in a separate, tool-free follow-up call rather than
     * on the tool-calling call itself: small local models reliably drift into free-form
     * prose once tool results are in context, which breaks JSON parsing of the entity()
     * response. Asking a plain formatting question against the finished investigation is
     * a much easier task and parses far more reliably.
     */
    private static final String STRUCTURE_SYSTEM_PROMPT = """
            Extract the diagnosis below into the required structured fields: rootCause (concise
            root cause), suggestion (concrete, actionable remediation), confidence (integer 0-100
            reflecting how certain the diagnosis is), diagnosisDetail (the full diagnosis
            narrative). Do not invent information beyond what's in the diagnosis.
            """;

    public String systemPrompt() {
        return SYSTEM_PROMPT;
    }

    public String structureSystemPrompt() {
        return STRUCTURE_SYSTEM_PROMPT;
    }

    public String userPrompt(AlertRecord alert) {
        return """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                Investigate this alert using the available tools as needed, then give your diagnosis.
                """.formatted(alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(),
                        alert.getLevel(), alert.getAlertContent(), alert.getTriggerTime());
    }

    public String retryUserPrompt(AlertRecord alert) {
        return """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                This is a retry: the previous investigation produced a low-confidence diagnosis.
                Widen your investigation — when calling queryDeviceHistory, use hours=2 (120
                minutes) to capture more historical context — then give your diagnosis.
                """.formatted(alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(),
                        alert.getLevel(), alert.getAlertContent(), alert.getTriggerTime());
    }
}
