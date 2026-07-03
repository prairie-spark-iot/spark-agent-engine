package com.spark.agent.repository;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.DeviceData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DeviceDataRepositoryTest {

    private static final String DEVICE_KEY = "DK_TEST_WINDOW_FN";

    @Autowired
    private DeviceDataRepository deviceDataRepository;

    @Autowired
    private SnowflakeIdGenerator idGenerator;

    private final List<Long> insertedIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        deviceDataRepository.deleteAllByIdInBatch(insertedIds);
        insertedIds.clear();
    }

    private DeviceData insertRow(String identifier, LocalDateTime reportTime, String value) {
        DeviceData d = new DeviceData();
        long id = idGenerator.nextId();
        d.setId(id);
        d.setDeviceId(1L);
        d.setDeviceKey(DEVICE_KEY);
        d.setIdentifier(identifier);
        d.setValue(value);
        d.setValueNum(new BigDecimal(value));
        d.setReportTime(reportTime);
        deviceDataRepository.save(d);
        insertedIds.add(id);
        return d;
    }

    @Test
    void findLatestByDeviceKey_returnsOneRowPerIdentifierWithNewestReportTime() {
        LocalDateTime now = LocalDateTime.now();
        insertRow("temperature", now.minusMinutes(10), "10");
        insertRow("temperature", now.minusMinutes(5), "20");
        insertRow("temperature", now, "30");
        insertRow("pressure", now.minusMinutes(3), "100");
        insertRow("pressure", now.minusMinutes(1), "200");

        List<DeviceData> latest = deviceDataRepository.findLatestByDeviceKey(DEVICE_KEY);

        Map<String, DeviceData> byIdentifier = latest.stream()
                .collect(Collectors.toMap(DeviceData::getIdentifier, d -> d));

        assertEquals(2, latest.size());
        assertEquals("30", byIdentifier.get("temperature").getValue());
        assertEquals("200", byIdentifier.get("pressure").getValue());
    }

    @Test
    void findLatestByDeviceKey_noRowsForDeviceKey_returnsEmpty() {
        List<DeviceData> latest = deviceDataRepository.findLatestByDeviceKey("DK_DOES_NOT_EXIST");

        assertTrue(latest.isEmpty());
    }
}
