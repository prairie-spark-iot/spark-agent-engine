package com.spark.agent.dto;

import com.spark.agent.entity.DeviceData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeviceLatestResponseTest {

    @Test
    void from_mapsFieldsFromDeviceData() {
        DeviceData data = new DeviceData();
        data.setDeviceKey("DK_TEST_001");
        data.setIdentifier("temperature");
        data.setValue("235.5");
        data.setValueNum(BigDecimal.valueOf(235.5));
        data.setQuality((short) 1);
        LocalDateTime reportTime = LocalDateTime.of(2026, 7, 3, 10, 0);
        data.setReportTime(reportTime);
        data.setTenantId(1L);
        data.setDeleted((short) 0);

        DeviceLatestResponse response = DeviceLatestResponse.from(data);

        assertEquals("DK_TEST_001", response.deviceKey());
        assertEquals("temperature", response.identifier());
        assertEquals("235.5", response.value());
        assertEquals(BigDecimal.valueOf(235.5), response.valueNum());
        assertEquals((short) 1, response.quality());
        assertEquals(reportTime, response.reportTime());
    }
}
