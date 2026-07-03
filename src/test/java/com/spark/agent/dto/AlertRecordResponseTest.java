package com.spark.agent.dto;

import com.spark.agent.entity.AlertRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlertRecordResponseTest {

    @Test
    void from_mapsFieldsFromAlertRecord() {
        AlertRecord record = new AlertRecord();
        record.setId(1L);
        record.setRuleId(2L);
        record.setDeviceId(3L);
        record.setDeviceKey("DK_TEST_001");
        record.setIdentifier("temperature");
        record.setTriggerValue("300.0");
        record.setLevel((short) 3);
        record.setAlertContent("Temperature too high");
        LocalDateTime triggerTime = LocalDateTime.of(2026, 7, 3, 9, 0);
        record.setTriggerTime(triggerTime);
        record.setDiagnosisStatus((short) 2);
        record.setHandleStatus((short) 1);
        record.setRootCause("Overheating");
        record.setSuggestion("Check cooling system");
        record.setConfidence(BigDecimal.valueOf(0.95));
        record.setDiagnosisDetail("detail");
        LocalDateTime diagnosisTime = LocalDateTime.of(2026, 7, 3, 9, 5);
        record.setDiagnosisTime(diagnosisTime);
        record.setTenantId(1L);
        record.setDeleted((short) 0);

        AlertRecordResponse response = AlertRecordResponse.from(record);

        assertEquals(1L, response.id());
        assertEquals(2L, response.ruleId());
        assertEquals(3L, response.deviceId());
        assertEquals("DK_TEST_001", response.deviceKey());
        assertEquals("temperature", response.identifier());
        assertEquals("300.0", response.triggerValue());
        assertEquals((short) 3, response.level());
        assertEquals("Temperature too high", response.alertContent());
        assertEquals(triggerTime, response.triggerTime());
        assertEquals((short) 2, response.diagnosisStatus());
        assertEquals((short) 1, response.handleStatus());
        assertEquals("Overheating", response.rootCause());
        assertEquals("Check cooling system", response.suggestion());
        assertEquals(BigDecimal.valueOf(0.95), response.confidence());
        assertEquals("detail", response.diagnosisDetail());
        assertEquals(diagnosisTime, response.diagnosisTime());
    }
}
