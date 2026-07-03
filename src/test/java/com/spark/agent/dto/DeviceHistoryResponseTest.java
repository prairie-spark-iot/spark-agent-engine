package com.spark.agent.dto;

import com.spark.agent.entity.DeviceData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeviceHistoryResponseTest {

    @Test
    void from_mapsFieldsFromDeviceData() {
        DeviceData data = new DeviceData();
        data.setDeviceKey("DK_TEST_001");
        data.setIdentifier("pressure");
        data.setValue("156.2");
        data.setValueNum(BigDecimal.valueOf(156.2));
        data.setQuality((short) 1);
        LocalDateTime reportTime = LocalDateTime.of(2026, 7, 3, 10, 5);
        data.setReportTime(reportTime);

        DeviceHistoryResponse response = DeviceHistoryResponse.from(data);

        assertEquals("DK_TEST_001", response.deviceKey());
        assertEquals("pressure", response.identifier());
        assertEquals("156.2", response.value());
        assertEquals(BigDecimal.valueOf(156.2), response.valueNum());
        assertEquals((short) 1, response.quality());
        assertEquals(reportTime, response.reportTime());
    }
}
