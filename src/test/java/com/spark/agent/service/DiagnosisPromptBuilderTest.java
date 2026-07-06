package com.spark.agent.service;

import com.spark.agent.entity.AlertRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosisPromptBuilderTest {

    private DiagnosisPromptBuilder builder;
    private AlertRecord alert;

    @BeforeEach
    void setUp() {
        builder = new DiagnosisPromptBuilder();

        alert = new AlertRecord();
        alert.setDeviceKey("DK_TEST");
        alert.setIdentifier("temperature");
        alert.setTriggerValue("150.5");
        alert.setLevel((short) 2);
        alert.setAlertContent("High temperature alert");
        alert.setTriggerTime(LocalDateTime.of(2026, 7, 3, 10, 30));
    }

    @Test
    void userPrompt_includesAllAlertFields() {
        String prompt = builder.userPrompt(alert);

        assertTrue(prompt.contains("DK_TEST"));
        assertTrue(prompt.contains("temperature"));
        assertTrue(prompt.contains("150.5"));
        assertTrue(prompt.contains("2"));
        assertTrue(prompt.contains("High temperature alert"));
        assertTrue(prompt.contains("2026-07-03T10:30"));
    }

    @Test
    void retryUserPrompt_includesAllAlertFieldsAndWidenWindowInstruction() {
        String prompt = builder.retryUserPrompt(alert);

        assertTrue(prompt.contains("DK_TEST"));
        assertTrue(prompt.contains("temperature"));
        assertTrue(prompt.contains("150.5"));
        assertTrue(prompt.contains("High temperature alert"));
        assertTrue(prompt.contains("2026-07-03T10:30"));
        assertTrue(prompt.contains("hours=2"));
        assertTrue(prompt.contains("120"));
    }

    @Test
    void systemPrompt_mentionsAvailableTools() {
        String prompt = builder.systemPrompt();

        assertFalse(prompt.isBlank());
        assertTrue(prompt.contains("queryDeviceStatus"));
        assertTrue(prompt.contains("queryDeviceHistory"));
        assertTrue(prompt.contains("queryDeviceAlerts"));
        assertTrue(prompt.contains("queryDeviceManual"));
    }

    @Test
    void structureSystemPrompt_requestsStructuredFields() {
        String prompt = builder.structureSystemPrompt();

        assertFalse(prompt.isBlank());
        assertTrue(prompt.contains("rootCause"));
        assertTrue(prompt.contains("suggestion"));
        assertTrue(prompt.contains("confidence"));
        assertTrue(prompt.contains("timeline"));
        assertTrue(prompt.contains("suggestedActionPlan"));
    }
}
