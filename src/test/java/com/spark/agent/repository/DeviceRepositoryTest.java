package com.spark.agent.repository;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.Device;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DeviceRepositoryTest {

    @Autowired
    private DeviceRepository deviceRepository;

    @Autowired
    private SnowflakeIdGenerator idGenerator;

    private final List<Long> insertedIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        deviceRepository.deleteAllByIdInBatch(insertedIds);
        insertedIds.clear();
    }

    private Device insertDevice(String deviceKey, short onlineStatus) {
        Device d = new Device();
        long id = idGenerator.nextId();
        d.setId(id);
        d.setProductId(1001L);
        d.setDeviceName("Test Device " + deviceKey);
        d.setDeviceKey(deviceKey);
        d.setOnlineStatus(onlineStatus);
        Device saved = deviceRepository.save(d);
        insertedIds.add(id);
        return saved;
    }

    @Test
    void findByOnlineStatusAndDeleted_returnsOnlyOnlineNonDeletedDevices() {
        insertDevice("DK_TEST_REPO_ON1", (short) 1);
        insertDevice("DK_TEST_REPO_ON2", (short) 1);
        insertDevice("DK_TEST_REPO_OFF1", (short) 0);

        List<Device> online = deviceRepository.findByOnlineStatusAndDeleted((short) 1, (short) 0);
        List<String> keys = online.stream().map(Device::getDeviceKey).toList();

        assertTrue(keys.contains("DK_TEST_REPO_ON1"));
        assertTrue(keys.contains("DK_TEST_REPO_ON2"));
        assertFalse(keys.contains("DK_TEST_REPO_OFF1"));
    }

    @Test
    void findByOnlineStatusAndDeleted_deviceMarkedDeleted_isExcluded() {
        Device d = insertDevice("DK_TEST_REPO_DELETED", (short) 1);
        d.setDeleted((short) 1);
        deviceRepository.save(d);

        List<Device> online = deviceRepository.findByOnlineStatusAndDeleted((short) 1, (short) 0);
        List<String> keys = online.stream().map(Device::getDeviceKey).toList();

        assertFalse(keys.contains("DK_TEST_REPO_DELETED"));
    }
}
