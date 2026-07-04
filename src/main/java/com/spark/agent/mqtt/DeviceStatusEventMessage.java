package com.spark.agent.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceStatusEventMessage {
    private String deviceKey;
}
