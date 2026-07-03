package com.spark.agent.dto;

import com.spark.agent.entity.DeviceData;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record DeviceHistoryResponse(
        String deviceKey,
        String identifier,
        String value,
        BigDecimal valueNum,
        Short quality,
        LocalDateTime reportTime
) {
    public static DeviceHistoryResponse from(DeviceData data) {
        return new DeviceHistoryResponse(
                data.getDeviceKey(),
                data.getIdentifier(),
                data.getValue(),
                data.getValueNum(),
                data.getQuality(),
                data.getReportTime()
        );
    }
}
