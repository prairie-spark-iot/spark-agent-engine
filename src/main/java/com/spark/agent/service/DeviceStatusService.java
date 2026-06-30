package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.Device;
import com.spark.agent.repository.DeviceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceStatusService {

    private final DeviceRepository deviceRepository;
    private final AppProperties appProperties;

    @Scheduled(fixedDelayString = "#{@appProperties.offlineTimeoutSeconds * 500}")
    @Transactional
    public void sweepOfflineDevices() {
        LocalDateTime cutoff = LocalDateTime.now()
                .minusSeconds(appProperties.getOfflineTimeoutSeconds());

        List<Long> staleIds = deviceRepository.findAllOnline().stream()
                .filter(d -> d.getLastOnlineTime() == null || d.getLastOnlineTime().isBefore(cutoff))
                .map(Device::getId)
                .toList();

        if (!staleIds.isEmpty()) {
            deviceRepository.markOfflineBatch(staleIds, LocalDateTime.now());
            log.info("[Status] Marked {} device(s) offline: {}", staleIds.size(), staleIds);
        }
    }
}
