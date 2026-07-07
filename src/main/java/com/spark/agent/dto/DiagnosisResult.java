package com.spark.agent.dto;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public record DiagnosisResult(
        String rootCause,
        String suggestion,
        int confidence,
        List<TimelineStep> timeline,
        List<ActionItem> suggestedActionPlan
) {
    public record TimelineStep(String title, String description) {
    }

    /**
     * The structuring prompt asks for "a short actionable sentence" per item, so small local
     * models frequently emit a bare JSON string instead of {"text": "..."} - accept both shapes
     * so a real diagnosis isn't discarded over formatting drift.
     */
    public record ActionItem(String text) {
        @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
        public static ActionItem fromText(String text) {
            return new ActionItem(text);
        }
    }
}
