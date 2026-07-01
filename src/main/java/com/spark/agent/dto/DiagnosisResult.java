package com.spark.agent.dto;

public record DiagnosisResult(String rootCause, String suggestion, int confidence, String diagnosisDetail) {
}
