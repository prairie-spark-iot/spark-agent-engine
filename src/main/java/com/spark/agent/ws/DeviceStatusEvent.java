package com.spark.agent.ws;

import java.time.LocalDateTime;

public record DeviceStatusEvent(String deviceKey, boolean online, LocalDateTime changedAt) {
}
