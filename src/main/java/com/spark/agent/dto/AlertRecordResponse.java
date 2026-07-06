package com.spark.agent.dto;

import com.spark.agent.entity.AlertRecord;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record AlertRecordResponse(
        Long id,
        Long ruleId,
        Long deviceId,
        String deviceKey,
        String identifier,
        String triggerValue,
        Short level,
        String alertContent,
        LocalDateTime triggerTime,
        Short diagnosisStatus,
        Short handleStatus,
        String rootCause,
        String suggestion,
        BigDecimal confidence,
        String diagnosisDetail,
        LocalDateTime diagnosisTime,
        String ruleOperator,
        String ruleThreshold,
        LocalDateTime diagnosisRequestedAt,
        LocalDateTime approvedAt
) {
    public static AlertRecordResponse from(AlertRecord record) {
        return new AlertRecordResponse(
                record.getId(),
                record.getRuleId(),
                record.getDeviceId(),
                record.getDeviceKey(),
                record.getIdentifier(),
                record.getTriggerValue(),
                record.getLevel(),
                record.getAlertContent(),
                record.getTriggerTime(),
                record.getDiagnosisStatus(),
                record.getHandleStatus(),
                record.getRootCause(),
                record.getSuggestion(),
                record.getConfidence(),
                record.getDiagnosisDetail(),
                record.getDiagnosisTime(),
                record.getRuleOperator(),
                record.getRuleThreshold(),
                record.getDiagnosisRequestedAt(),
                record.getApprovedAt()
        );
    }
}
