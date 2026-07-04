package com.spark.agent.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceConnectionEventMessage {
    private String deviceKey;
    private String status;
    private Long timestamp;
}
