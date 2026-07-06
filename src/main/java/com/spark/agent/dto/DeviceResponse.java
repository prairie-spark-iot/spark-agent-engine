package com.spark.agent.dto;

import java.util.List;

public record DeviceResponse(
        String deviceKey,
        String deviceName,
        String productKey,
        boolean online,
        List<DeviceLatestResponse> telemetry
) {
}
