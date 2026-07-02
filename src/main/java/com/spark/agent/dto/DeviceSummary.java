package com.spark.agent.dto;

public record DeviceSummary(
        String deviceKey,
        String deviceName,
        String productKey,
        boolean online
) {
}
