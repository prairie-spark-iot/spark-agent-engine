package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.DeviceData;
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
    public R<List<DeviceData>> latest(@PathVariable String deviceKey) {
        return R.ok(deviceDataRepository.findLatestByDeviceKey(deviceKey));
    }

    /** History for one identifier (default last 50) */
    @GetMapping("/device/{deviceKey}/history")
    public R<List<DeviceData>> history(
            @PathVariable String deviceKey,
            @RequestParam String identifier,
            @RequestParam(defaultValue = "50") int limit) {
        List<DeviceData> rows = deviceDataRepository
                .findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
                        deviceKey, identifier, (short) 0, PageRequest.of(0, limit));
        return R.ok(rows);
    }

    /** Most recent alert records */
    @GetMapping("/alert/recent")
    public R<List<AlertRecord>> recentAlerts(
            @RequestParam(defaultValue = "20") int limit) {
        return R.ok(alertRecordRepository.findByDeletedOrderByTriggerTimeDesc(
                (short) 0, PageRequest.of(0, limit)));
    }
}
