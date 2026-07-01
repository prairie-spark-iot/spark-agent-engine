package com.spark.agent.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Simple snowflake ID: 41-bit timestamp | 10-bit machine | 12-bit sequence
 */
@Component
public class SnowflakeIdGenerator {

    private static final long EPOCH = 1609459200000L; // 2021-01-01 UTC
    private static final long MACHINE_BITS = 10;
    private static final long SEQUENCE_BITS = 12;
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);
    private static final long MACHINE_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = MACHINE_BITS + SEQUENCE_BITS;

    private final long machineId;
    private long lastTimestamp = -1;
    private long sequence = 0;

    public SnowflakeIdGenerator(@Value("${app.snowflake.machine-id:1}") long machineId) {
        this.machineId = machineId;
    }

    public synchronized long nextId() {
        long ts = System.currentTimeMillis() - EPOCH;
        if (ts < lastTimestamp) {
            throw new IllegalStateException(
                    "Clock moved backwards. Refusing to generate ID for %d ms".formatted(lastTimestamp - ts));
        }
        if (ts == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                while (ts <= lastTimestamp) {
                    ts = System.currentTimeMillis() - EPOCH;
                }
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = ts;
        return (ts << TIMESTAMP_SHIFT) | (machineId << MACHINE_SHIFT) | sequence;
    }
}
