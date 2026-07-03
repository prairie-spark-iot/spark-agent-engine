package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.AlertOperator;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.AlertRule;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.AlertRuleRepository;
import com.spark.agent.repository.DeviceRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class AlertServiceConcurrencyTest {

    private static final String DEVICE_KEY = "DK_TEST_ALERT_RACE";

    @Autowired
    private AlertService alertService;
    @Autowired
    private DeviceRepository deviceRepository;
    @Autowired
    private AlertRuleRepository alertRuleRepository;
    @Autowired
    private AlertRecordRepository alertRecordRepository;
    @Autowired
    private SnowflakeIdGenerator idGenerator;

    private Long deviceId;
    private Long ruleId;

    @AfterEach
    void cleanUp() {
        if (ruleId != null && deviceId != null) {
            alertRecordRepository.findByRuleIdAndDeviceId(ruleId, deviceId)
                    .forEach(r -> alertRecordRepository.deleteById(r.getId()));
        }
        if (ruleId != null) {
            alertRuleRepository.deleteById(ruleId);
        }
        if (deviceId != null) {
            deviceRepository.deleteById(deviceId);
        }
    }

    @Test
    void evaluate_concurrentCallsForSameDeviceAndRule_onlyOneAlertRecordCreated() throws Exception {
        Device device = new Device();
        device.setId(idGenerator.nextId());
        device.setProductId(1L);
        device.setDeviceName("Alert Race Test Device");
        device.setDeviceKey(DEVICE_KEY);
        deviceRepository.save(device);
        deviceId = device.getId();

        AlertRule rule = new AlertRule();
        rule.setId(idGenerator.nextId());
        rule.setName("concurrency-test-rule");
        rule.setDeviceId(deviceId);
        rule.setIdentifier("temperature");
        rule.setOperator(AlertOperator.GT);
        rule.setThreshold("100");
        rule.setLevel((short) 2);
        rule.setStatus((short) 1);
        alertRuleRepository.save(rule);
        ruleId = rule.getId();

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                DeviceData data = new DeviceData();
                data.setId(idGenerator.nextId());
                data.setDeviceId(deviceId);
                data.setDeviceKey(DEVICE_KEY);
                data.setIdentifier("temperature");
                data.setValue("150.5");
                data.setValueNum(new BigDecimal("150.5"));
                data.setReportTime(LocalDateTime.now());

                ready.countDown();
                go.await();
                alertService.evaluate(data);
                return null;
            }));
        }

        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        List<AlertRecord> created = alertRecordRepository.findByRuleIdAndDeviceId(ruleId, deviceId);
        assertEquals(1, created.size(), "concurrent evaluate() calls for the same device+rule must produce exactly one alert record");
    }
}
