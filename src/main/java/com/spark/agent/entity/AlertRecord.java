package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "aiot_alert_record")
public class AlertRecord extends BaseEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "rule_id")
    private Long ruleId;

    @Column(name = "device_id", nullable = false)
    private Long deviceId;

    @Column(name = "device_key", nullable = false)
    private String deviceKey;

    @Column(name = "identifier")
    private String identifier;

    @Column(name = "trigger_value")
    private String triggerValue;

    /** denormalized copy of the matched AlertRule's operator at trigger time — see sql/2026-07-06-alert-record-diagnose-columns.sql */
    @Column(name = "rule_operator")
    private String ruleOperator;

    /** denormalized copy of the matched AlertRule's threshold at trigger time — see sql/2026-07-06-alert-record-diagnose-columns.sql */
    @Column(name = "rule_threshold")
    private String ruleThreshold;

    /** 1=info 2=warning 3=critical */
    @Column(name = "level", nullable = false)
    private Short level = 1;

    @Column(name = "alert_content")
    private String alertContent;

    @Column(name = "trigger_time", nullable = false)
    private LocalDateTime triggerTime;

    /** 0=pending 1=human_review_required 2=diagnosed (auto, confidence above gate) */
    @Column(name = "diagnosis_status", nullable = false)
    private Short diagnosisStatus = 0;

    /** 0=unhandled 1=handled */
    @Column(name = "handle_status", nullable = false)
    private Short handleStatus = 0;

    // AI diagnosis fields
    @Column(name = "root_cause", columnDefinition = "text")
    private String rootCause;

    @Column(name = "suggestion", columnDefinition = "text")
    private String suggestion;

    @Column(name = "confidence", precision = 5, scale = 2)
    private BigDecimal confidence;

    @Column(name = "diagnosis_detail", columnDefinition = "text")
    private String diagnosisDetail;

    @Column(name = "diagnosis_time")
    private LocalDateTime diagnosisTime;

    /** set when an on-demand diagnosis is requested via POST /api/alerts/{id}/diagnose; drives the transient "Diagnosing" state */
    @Column(name = "diagnosis_requested_at")
    private LocalDateTime diagnosisRequestedAt;

    /** set the first time POST /api/alerts/{id}/approve succeeds — see sql/2026-07-06-alert-record-approved-at.sql */
    @Column(name = "approved_at")
    private LocalDateTime approvedAt;
}
