package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

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

    /** 1=info 2=warning 3=critical */
    @Column(name = "level", nullable = false)
    private Short level = 1;

    @Column(name = "alert_content")
    private String alertContent;

    @Column(name = "trigger_time", nullable = false)
    private LocalDateTime triggerTime;

    /** 0=pending 1=diagnosed */
    @Column(name = "diagnosis_status", nullable = false)
    private Short diagnosisStatus = 0;

    /** 0=unhandled 1=handled */
    @Column(name = "handle_status", nullable = false)
    private Short handleStatus = 0;

    // AI diagnosis fields — populated in a later phase
    @Column(name = "root_cause", columnDefinition = "text")
    private String rootCause;

    @Column(name = "suggestion", columnDefinition = "text")
    private String suggestion;
}
