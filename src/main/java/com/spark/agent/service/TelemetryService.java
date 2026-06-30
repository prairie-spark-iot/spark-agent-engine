package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.mqtt.DeviceTelemetryMessage;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TelemetryService {

    private final DeviceRepository deviceRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final AlertService alertService;
    private final KafkaProducerService kafkaProducerService;
    private final SnowflakeIdGenerator idGenerator;

    @Transactional
    public void process(DeviceTelemetryMessage msg) {
        Optional<Device> deviceOpt = deviceRepository.findByDeviceKeyAndDeleted(msg.getDeviceKey(), (short) 0);
        if (deviceOpt.isEmpty()) {
            log.warn("[Telemetry] Unknown device: {}", msg.getDeviceKey());
            return;
        }
        Device device = deviceOpt.get();
        LocalDateTime reportTime = toLocalDateTime(msg.getTimestamp());

        deviceRepository.markOnline(device.getId(), LocalDateTime.now());

        List<DeviceData> rows = buildRows(msg, device.getId(), reportTime);
        deviceDataRepository.saveAll(rows);

        for (DeviceData row : rows) {
            kafkaProducerService.sendTelemetry(row);
            alertService.evaluate(row);
        }

        log.debug("[Telemetry] Processed {} properties for {}", rows.size(), msg.getDeviceKey());
    }

    private List<DeviceData> buildRows(DeviceTelemetryMessage msg, Long deviceId, LocalDateTime reportTime) {
        List<DeviceData> rows = new ArrayList<>();
        for (Map.Entry<String, Object> entry : msg.getProperties().entrySet()) {
            DeviceData data = new DeviceData();
            data.setId(idGenerator.nextId());
            data.setDeviceId(deviceId);
            data.setDeviceKey(msg.getDeviceKey());
            data.setIdentifier(entry.getKey());
            data.setReportTime(reportTime);
            data.setQuality((short) 1);

            Object val = entry.getValue();
            data.setValue(String.valueOf(val));
            if (val instanceof Number n) {
                data.setValueNum(BigDecimal.valueOf(n.doubleValue()));
            }
            rows.add(data);
        }
        return rows;
    }

    private LocalDateTime toLocalDateTime(Long epochMillis) {
        if (epochMillis == null) return LocalDateTime.now();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
    }
}
