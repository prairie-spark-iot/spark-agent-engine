package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.Device;
import com.spark.agent.repository.DeviceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeviceHeartbeatServiceTest {

    @Mock private StringRedisTemplate redisTemplate;
    @Mock private DeviceRepository deviceRepository;

    private final AppProperties appProperties = new AppProperties();
    private DeviceHeartbeatService service;

    @BeforeEach
    void setUp() {
        service = new DeviceHeartbeatService(redisTemplate, deviceRepository, appProperties);
    }

    @Test
    void markOfflineNow_deviceExists_deletesKeyAndMarksOffline() {
        Device device = new Device();
        device.setId(42L);
        device.setDeviceKey("DK_TEST_001");
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0))
                .thenReturn(Optional.of(device));

        service.markOfflineNow("DK_TEST_001");

        verify(redisTemplate).delete("device:online:DK_TEST_001");
        verify(deviceRepository).markOffline(eq(42L), any(LocalDateTime.class));
    }

    @Test
    void markOfflineNow_unknownDevice_deletesKeyButDoesNotMarkOffline() {
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_UNKNOWN", (short) 0))
                .thenReturn(Optional.empty());

        service.markOfflineNow("DK_UNKNOWN");

        verify(redisTemplate).delete("device:online:DK_UNKNOWN");
        verify(deviceRepository, never()).markOffline(any(), any());
    }

    @Test
    void markAllOffline_multipleOnlineDevices_marksEachOfflineAndDeletesEachKey() {
        Device d1 = new Device();
        d1.setId(1L);
        d1.setDeviceKey("DK_TEST_A");
        Device d2 = new Device();
        d2.setId(2L);
        d2.setDeviceKey("DK_TEST_B");
        when(deviceRepository.findByOnlineStatusAndDeleted((short) 1, (short) 0))
                .thenReturn(List.of(d1, d2));

        service.markAllOffline();

        verify(redisTemplate).delete("device:online:DK_TEST_A");
        verify(redisTemplate).delete("device:online:DK_TEST_B");
        verify(deviceRepository).markOffline(eq(1L), any(LocalDateTime.class));
        verify(deviceRepository).markOffline(eq(2L), any(LocalDateTime.class));
    }

    @Test
    void markAllOffline_noOnlineDevices_doesNothing() {
        when(deviceRepository.findByOnlineStatusAndDeleted((short) 1, (short) 0))
                .thenReturn(List.of());

        service.markAllOffline();

        verifyNoInteractions(redisTemplate);
        verify(deviceRepository, never()).markOffline(any(), any());
    }
}
