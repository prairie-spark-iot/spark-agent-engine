package com.spark.agent.dto;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiagnosisResultTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void actionItem_deserializesFromObjectForm() {
        DiagnosisResult.ActionItem item =
                objectMapper.readValue("{\"text\":\"Inspect coolant flow\"}", DiagnosisResult.ActionItem.class);

        assertEquals("Inspect coolant flow", item.text());
    }

    @Test
    void actionItem_deserializesFromBareStringForm() {
        // The structuring prompt asks for "an ordered list of ... each a short actionable
        // sentence" - small local models (qwen2.5:7b via Ollama) read that literally and often
        // emit a bare JSON string per item instead of the {"text": "..."} object the record's
        // canonical constructor expects, which previously threw MismatchedInputException and
        // discarded an otherwise-valid diagnosis.
        DiagnosisResult.ActionItem item =
                objectMapper.readValue("\"Inspect coolant flow\"", DiagnosisResult.ActionItem.class);

        assertEquals("Inspect coolant flow", item.text());
    }

    @Test
    void actionItem_alwaysSerializesToObjectForm() {
        // aiot_alert_record.diagnosis_detail is a persisted contract the frontend adapter parses
        // as {text: string}[] (lib/adapters/alertAdapter.ts) - accepting bare strings on the way
        // in from the LLM must not change what we write back out.
        String json = objectMapper.writeValueAsString(new DiagnosisResult.ActionItem("Inspect coolant flow"));

        assertTrue(json.contains("\"text\""), "expected object form with a text field, got: " + json);
    }

    @Test
    void suggestedActionPlan_deserializesMixedArrayOfBareStringsAndObjects() {
        String json = """
                {"rootCause":"Overheating","suggestion":"Lower load","confidence":80,"timeline":[],
                 "suggestedActionPlan":["Lower pressure setpoint",{"text":"Inspect coolant flow"}]}
                """;

        DiagnosisResult result = objectMapper.readValue(json, DiagnosisResult.class);

        assertEquals(List.of(
                new DiagnosisResult.ActionItem("Lower pressure setpoint"),
                new DiagnosisResult.ActionItem("Inspect coolant flow")
        ), result.suggestedActionPlan());
    }
}
