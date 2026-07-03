package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.dto.AlertRecordResponse;
import com.spark.agent.dto.DeviceHistoryResponse;
import com.spark.agent.dto.DeviceLatestResponse;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ApiControllerTest {

    @Mock
    private DeviceDataRepository deviceDataRepository;

    @Mock
    private AlertRecordRepository alertRecordRepository;

    @InjectMocks
    private ApiController controller;

    @Test
    void latest_returnsData() {
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST"))
                .thenReturn(List.of(new DeviceData()));

        var result = controller.latest("DK_TEST");

        assertEquals(0, result.getCode());
        assertEquals("success", result.getMsg());
        assertFalse(result.getData().isEmpty());
    }

    @Test
    void latest_returnsDtoNotJpaEntity() {
        DeviceData entity = new DeviceData();
        entity.setDeviceKey("DK_TEST");
        entity.setIdentifier("temperature");
        entity.setDeleted((short) 0);
        entity.setTenantId(1L);
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of(entity));

        R<List<DeviceLatestResponse>> result = controller.latest("DK_TEST");

        assertEquals("temperature", result.getData().get(0).identifier());
    }

    @Test
    void history_returnsDtoNotJpaEntity() {
        DeviceData entity = new DeviceData();
        entity.setDeviceKey("DK_TEST");
        entity.setIdentifier("temperature");
        when(deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
                anyString(), anyString(), anyShort(), any()))
                .thenReturn(List.of(entity));

        R<List<DeviceHistoryResponse>> result = controller.history("DK_TEST", "temperature", 50);

        assertEquals("temperature", result.getData().get(0).identifier());
    }

    @Test
    void recentAlerts_returnsDtoNotJpaEntity() {
        AlertRecord entity = new AlertRecord();
        entity.setDeviceKey("DK_TEST");
        entity.setLevel((short) 2);
        entity.setDeleted((short) 0);
        entity.setTenantId(1L);
        when(alertRecordRepository.findByDeletedOrderByTriggerTimeDesc(anyShort(), any()))
                .thenReturn(List.of(entity));

        R<List<AlertRecordResponse>> result = controller.recentAlerts(20);

        assertEquals("DK_TEST", result.getData().get(0).deviceKey());
    }

    @Test
    void latest_noData_returns404() {
        when(deviceDataRepository.findLatestByDeviceKey("UNKNOWN")).thenReturn(List.of());

        var result = controller.latest("UNKNOWN");

        assertEquals(404, result.getCode());
        assertTrue(result.getMsg().contains("UNKNOWN"));
    }

    @Test
    void history_defaultLimit() {
        when(deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
                anyString(), anyString(), anyShort(), any()))
                .thenReturn(List.of());

        var result = controller.history("DK_TEST", "temperature", 50);
        assertEquals(0, result.getCode());
    }

    @Test
    void history_capsLimitAt500() {
        when(deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
                eq("DK_TEST"), eq("temperature"), eq((short) 0), argThat(pr ->
                        ((PageRequest) pr).getPageSize() == 500)))
                .thenReturn(List.of());

        var result = controller.history("DK_TEST", "temperature", 9999);
        assertEquals(0, result.getCode());
    }

    @Test
    void history_clampsLimitToMinimum1() {
        when(deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
                eq("DK_TEST"), eq("temperature"), eq((short) 0), argThat(pr ->
                        ((PageRequest) pr).getPageSize() == 1)))
                .thenReturn(List.of());

        var result = controller.history("DK_TEST", "temperature", 0);
        assertEquals(0, result.getCode());
    }

    @Test
    void recentAlerts_defaultLimit() {
        when(alertRecordRepository.findByDeletedOrderByTriggerTimeDesc(anyShort(), any()))
                .thenReturn(List.of());

        var result = controller.recentAlerts(20);
        assertEquals(0, result.getCode());
    }

    @Test
    void recentAlerts_capsLimitAt500() {
        when(alertRecordRepository.findByDeletedOrderByTriggerTimeDesc(
                eq((short) 0), argThat(pr ->
                        ((PageRequest) pr).getPageSize() == 500)))
                .thenReturn(List.of());

        var result = controller.recentAlerts(9999);
        assertEquals(0, result.getCode());
    }
}
