package com.spark.agent.dto;

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

    public record ActionItem(String text) {
    }
}
