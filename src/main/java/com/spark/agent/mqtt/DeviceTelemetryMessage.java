package com.spark.agent.mqtt;

import lombok.Data;

import java.util.Map;

@Data
public class DeviceTelemetryMessage {
    private String deviceKey;
    private String productKey;
    private Long timestamp;
    private Map<String, Object> properties;
}
