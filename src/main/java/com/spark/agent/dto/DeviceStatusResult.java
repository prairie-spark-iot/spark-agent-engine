package com.spark.agent.dto;

import com.spark.agent.entity.DeviceData;

import java.time.LocalDateTime;
import java.util.List;

public record DeviceStatusResult(
        String deviceKey,
        boolean found,
        String deviceName,
        String productKey,
        boolean online,
        LocalDateTime lastOnlineTime,
        LocalDateTime lastOfflineTime,
        List<DeviceData> latestTelemetry
) {
    public static DeviceStatusResult notFound(String deviceKey) {
        return new DeviceStatusResult(deviceKey, false, null, null, false, null, null, List.of());
    }
}
