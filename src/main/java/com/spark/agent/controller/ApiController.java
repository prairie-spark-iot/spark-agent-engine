package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.dto.AlertRecordResponse;
import com.spark.agent.dto.DeviceHistoryResponse;
import com.spark.agent.dto.DeviceLatestResponse;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ApiController {

    private final DeviceDataRepository deviceDataRepository;
    private final AlertRecordRepository alertRecordRepository;

    /** Latest value per identifier for the given device */
    @GetMapping("/device/{deviceKey}/latest")
    public R<List<DeviceLatestResponse>> latest(@PathVariable String deviceKey) {
        List<DeviceLatestResponse> result = deviceDataRepository.findLatestByDeviceKey(deviceKey)
                .stream().map(DeviceLatestResponse::from).toList();
        if (result.isEmpty()) {
            return R.fail(404, "Device key not found or has no data: " + deviceKey);
        }
        return R.ok(result);
    }

    private static final int MAX_LIMIT = 500;

    /** History for one identifier (default last 50, max 500) */
    @GetMapping("/device/{deviceKey}/history")
    public R<List<DeviceHistoryResponse>> history(
            @PathVariable String deviceKey,
            @RequestParam String identifier,
            @RequestParam(defaultValue = "50") int limit) {
        int capped = Math.min(Math.max(limit, 1), MAX_LIMIT);
        List<DeviceHistoryResponse> rows = deviceDataRepository
                .findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
                        deviceKey, identifier, (short) 0, PageRequest.of(0, capped))
                .stream().map(DeviceHistoryResponse::from).toList();
        return R.ok(rows);
    }

    /** Most recent alert records (max 500) */
    @GetMapping("/alert/recent")
    public R<List<AlertRecordResponse>> recentAlerts(
            @RequestParam(defaultValue = "20") int limit) {
        int capped = Math.min(Math.max(limit, 1), MAX_LIMIT);
        List<AlertRecordResponse> rows = alertRecordRepository
                .findByDeletedOrderByTriggerTimeDesc((short) 0, PageRequest.of(0, capped))
                .stream().map(AlertRecordResponse::from).toList();
        return R.ok(rows);
    }
}
