package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.mqtt.DeviceTelemetryMessage;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TelemetryServiceTest {

    @Mock private DeviceRepository deviceRepository;
    @Mock private DeviceDataRepository deviceDataRepository;
    @Mock private AlertService alertService;
    @Mock private OutboxMessageRepository outboxMessageRepository;
    @Mock private OutboxMessageFactory outboxMessageFactory;
    @Mock private SnowflakeIdGenerator idGenerator;
    @Mock private DeviceHeartbeatService heartbeatService;

    private TelemetryService telemetryService;

    private Device sampleDevice;
    private DeviceTelemetryMessage sampleMsg;

    @BeforeEach
    void setUp() {
        telemetryService = new TelemetryService(deviceRepository, deviceDataRepository, alertService,
                outboxMessageRepository, outboxMessageFactory, idGenerator, heartbeatService);

        sampleDevice = new Device();
        sampleDevice.setId(1L);
        sampleDevice.setDeviceKey("DK_TEST_001");

        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("temperature", 235.5);
        properties.put("pressure", 156.2);

        sampleMsg = new DeviceTelemetryMessage();
        sampleMsg.setDeviceKey("DK_TEST_001");
        sampleMsg.setProductKey("PK_TEST");
        sampleMsg.setTimestamp(1719655200000L);
        sampleMsg.setProperties(properties);
    }

    @Test
    void process_unknownDevice_doesNothing() {
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0)).thenReturn(Optional.empty());

        telemetryService.process(sampleMsg);

        verifyNoInteractions(deviceDataRepository, outboxMessageRepository, alertService, heartbeatService);
    }

    @Test
    void process_knownDevice_savesDataAndOneOutboxRowPerProperty() {
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0)).thenReturn(Optional.of(sampleDevice));
        when(idGenerator.nextId()).thenReturn(1001L, 1002L);
        when(outboxMessageFactory.build(eq("device_data"), any(), eq("device.data"), any(DeviceData.class)))
                .thenAnswer(inv -> new OutboxMessage());

        telemetryService.process(sampleMsg);

        verify(heartbeatService).heartbeat(1L, "DK_TEST_001");
        verify(deviceDataRepository).saveAll(anyList());
        verify(deviceDataRepository).flush();
        verify(alertService, times(2)).evaluate(any(DeviceData.class));

        ArgumentCaptor<List<OutboxMessage>> outboxCaptor = ArgumentCaptor.forClass(List.class);
        verify(outboxMessageRepository).saveAll(outboxCaptor.capture());
        assertEquals(2, outboxCaptor.getValue().size());

        verify(outboxMessageFactory, times(2))
                .build(eq("device_data"), any(), eq("device.data"), any(DeviceData.class));
    }

    @Test
    void process_emptyProperties_noRowsNoOutboxNoAlertEvaluation() {
        sampleMsg.setProperties(Map.of());
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0)).thenReturn(Optional.of(sampleDevice));

        telemetryService.process(sampleMsg);

        verify(deviceDataRepository).saveAll(List.of());
        verify(outboxMessageRepository).saveAll(List.of());
        verifyNoInteractions(alertService);
        verify(outboxMessageFactory, never()).build(any(), any(), any(), any());
    }
}
