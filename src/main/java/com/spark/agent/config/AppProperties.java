package com.spark.agent.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private int alertDebounceMinutes = 5;
    private int deviceHeartbeatTtlSeconds = 30;
    private String deviceHeartbeatKeyPrefix = "device:online:";

    /** confidence (0-100) at/above which a diagnosis is accepted without human review */
    private int diagnosisConfidenceThreshold = 80;

    /** confidence (0-100) below which diagnose() retries once with a wider (120-min) history window */
    private int diagnosisRetryConfidenceThreshold = 40;

    private int outboxRelayIntervalMs = 2000;
    private int outboxRelayBatchSize = 100;
}
