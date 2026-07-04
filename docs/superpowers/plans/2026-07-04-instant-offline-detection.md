# Instant Device Connectivity Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `spark-agent-engine` react immediately to the `spark-iot-emulator`'s explicit `device/online`, `device/offline` (including its crash LWT), and `thing/event/status/post` MQTT topics, instead of relying solely on passive Redis TTL expiry for offline detection.

**Architecture:** Add three more MQTT subscriptions to the existing single `Mqtt5AsyncClient` in `MqttSubscriber`, each with its own callback. `device/offline/+` catches both per-device offline events and the emulator's LWT crash message (`device/offline/emulator`) via one wildcard subscription, distinguished by payload `deviceKey`. New `DeviceHeartbeatService` methods (`markOfflineNow`, `markAllOffline`) perform immediate DB writes + Redis key cleanup; online/status events reuse the existing `heartbeat()` method. The existing Redis TTL mechanism is untouched and remains the fallback.

**Tech Stack:** Java 25, Spring Boot 4.1.0, Spring Data JPA, Spring Data Redis, HiveMQ MQTT client, JUnit 5 + Mockito, Jackson 3.

## Global Constraints

- Use `./gradlew`, not system gradle (per project CLAUDE.md).
- Jackson 3: `ObjectMapper`/core types come from `tools.jackson.*`; annotations (`@JsonIgnoreProperties`, `@JsonProperty`, etc.) stay at `com.fasterxml.jackson.annotation` — unchanged package.
- No schema changes — this feature persists nothing new; uptime values from `status/post` are read and discarded, never stored.
- `thing/event/alert/post` (device-side alerts) stays out of scope — the engine computes its own alerts independently via `AlertService`.
- The existing Redis TTL heartbeat mechanism (`DeviceHeartbeatService.heartbeat()` / `onMessage()`) is not modified — it remains the fallback for silent disconnects with no explicit MQTT event.
- Message handler error handling must match the existing `MqttSubscriber.dispatch()` pattern exactly: `try { ... } catch (Exception e) { log.error(...); }` — never rethrow, never block other topics' processing.
- Follow existing test conventions: Mockito unit tests (`@ExtendWith(MockitoExtension.class)`, `@Mock` fields) for services and `MqttSubscriber`; `@SpringBootTest` against the real dev Postgres for repository tests (see `DeviceDataRepositoryTest` for the pattern — insert temp rows, clean up in `@AfterEach`).
- Reference spec: `docs/superpowers/specs/2026-07-04-instant-offline-detection-design.md`.

---

### Task 1: `DeviceRepository.findByOnlineStatusAndDeleted`

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/DeviceRepository.java`
- Test: `src/test/java/com/spark/agent/repository/DeviceRepositoryTest.java` (new file)

**Interfaces:**
- Produces: `List<Device> findByOnlineStatusAndDeleted(Short onlineStatus, Short deleted)` on `DeviceRepository` — used by Task 2's `DeviceHeartbeatService.markAllOffline()`.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/spark/agent/repository/DeviceRepositoryTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.repository.DeviceRepositoryTest"`
Expected: FAIL — compilation error, `cannot find symbol: method findByOnlineStatusAndDeleted`

- [ ] **Step 3: Add the query method**

In `src/main/java/com/spark/agent/repository/DeviceRepository.java`, add below the existing `findByDeleted` method (after line 16):

```java
    List<Device> findByOnlineStatusAndDeleted(Short onlineStatus, Short deleted);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.repository.DeviceRepositoryTest"`
Expected: PASS — 2 tests completed, 0 failed (requires the dev Postgres from `spark-ai-infra` running on `localhost:5432`)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/repository/DeviceRepository.java src/test/java/com/spark/agent/repository/DeviceRepositoryTest.java
git commit -m "feat(repository): add findByOnlineStatusAndDeleted query for bulk offline marking"
```

---

### Task 2: `DeviceHeartbeatService.markOfflineNow` and `markAllOffline`

**Files:**
- Modify: `src/main/java/com/spark/agent/service/DeviceHeartbeatService.java`
- Test: `src/test/java/com/spark/agent/service/DeviceHeartbeatServiceTest.java` (new file)

**Interfaces:**
- Consumes: `DeviceRepository.findByOnlineStatusAndDeleted(Short, Short)` from Task 1; existing `DeviceRepository.findByDeviceKeyAndDeleted(String, Short)` and `DeviceRepository.markOffline(Long, LocalDateTime)`.
- Produces: `void markOfflineNow(String deviceKey)` and `void markAllOffline()` on `DeviceHeartbeatService` — used by Task 3's `MqttSubscriber.dispatchOffline()`.

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/com/spark/agent/service/DeviceHeartbeatServiceTest.java`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.DeviceHeartbeatServiceTest"`
Expected: FAIL — compilation error, `cannot find symbol: method markOfflineNow` / `markAllOffline`

- [ ] **Step 3: Implement the two methods**

In `src/main/java/com/spark/agent/service/DeviceHeartbeatService.java`, add after the existing `heartbeat()` method (after line 43, before `onMessage`):

```java
    /**
     * Immediately marks a device offline (bypassing the Redis TTL wait), triggered by an
     * explicit device/offline/{deviceKey} MQTT event.
     */
    @Transactional
    public void markOfflineNow(String deviceKey) {
        String key = appProperties.getDeviceHeartbeatKeyPrefix() + deviceKey;
        redisTemplate.delete(key);
        deviceRepository.findByDeviceKeyAndDeleted(deviceKey, (short) 0)
                .ifPresent(device -> {
                    deviceRepository.markOffline(device.getId(), LocalDateTime.now());
                    log.info("[Heartbeat] {} went offline (explicit event)", deviceKey);
                });
    }

    /**
     * Marks every currently-online device offline, triggered by the emulator's crash LWT
     * (device/offline/emulator) — one process death affects every device it was managing.
     */
    @Transactional
    public void markAllOffline() {
        LocalDateTime now = LocalDateTime.now();
        deviceRepository.findByOnlineStatusAndDeleted((short) 1, (short) 0)
                .forEach(device -> {
                    redisTemplate.delete(appProperties.getDeviceHeartbeatKeyPrefix() + device.getDeviceKey());
                    deviceRepository.markOffline(device.getId(), now);
                    log.info("[Heartbeat] {} went offline (emulator crash)", device.getDeviceKey());
                });
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.DeviceHeartbeatServiceTest"`
Expected: PASS — 4 tests completed, 0 failed

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/service/DeviceHeartbeatService.java src/test/java/com/spark/agent/service/DeviceHeartbeatServiceTest.java
git commit -m "feat(heartbeat): add markOfflineNow and markAllOffline for instant offline detection"
```

---

### Task 3: Wire up `device/online`, `device/offline`, and status MQTT subscriptions

**Files:**
- Create: `src/main/java/com/spark/agent/mqtt/DeviceConnectionEventMessage.java`
- Create: `src/main/java/com/spark/agent/mqtt/DeviceStatusEventMessage.java`
- Modify: `src/main/java/com/spark/agent/config/MqttProperties.java`
- Modify: `src/main/java/com/spark/agent/mqtt/MqttSubscriber.java`
- Modify: `src/main/resources/application.yaml`
- Modify: `CLAUDE.md`
- Test: `src/test/java/com/spark/agent/mqtt/MqttSubscriberTest.java`

**Interfaces:**
- Consumes: `DeviceHeartbeatService.markOfflineNow(String)` and `.markAllOffline()` from Task 2; existing `DeviceHeartbeatService.heartbeat(Long, String)`; existing `DeviceRepository.findByDeviceKeyAndDeleted(String, Short)`.
- Produces: `MqttSubscriber.dispatchOnline(Mqtt5Publish)`, `.dispatchOffline(Mqtt5Publish)`, `.dispatchStatus(Mqtt5Publish)` — package-private, directly testable like the existing `dispatch()`.

- [ ] **Step 1: Create the two new DTOs**

Create `src/main/java/com/spark/agent/mqtt/DeviceConnectionEventMessage.java`:

```java
package com.spark.agent.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceConnectionEventMessage {
    private String deviceKey;
    private String status;
    private Long timestamp;
}
```

Create `src/main/java/com/spark/agent/mqtt/DeviceStatusEventMessage.java`:

```java
package com.spark.agent.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceStatusEventMessage {
    private String deviceKey;
}
```

- [ ] **Step 2: Add the three new topic properties**

In `src/main/java/com/spark/agent/config/MqttProperties.java`, add after the existing `topic` field (after line 13):

```java
    private String onlineTopic = "device/online/+";
    private String offlineTopic = "device/offline/+";
    private String statusTopic = "/sys/+/+/thing/event/status/post";
```

- [ ] **Step 3: Write the failing tests for the new dispatch methods**

In `src/test/java/com/spark/agent/mqtt/MqttSubscriberTest.java`, replace the full file with:

```java
package com.spark.agent.mqtt;

import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.spark.agent.config.MqttProperties;
import com.spark.agent.entity.Device;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.service.DeviceHeartbeatService;
import com.spark.agent.service.TelemetryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttSubscriberTest {

    @Mock private TelemetryService telemetryService;
    @Mock private DeviceRepository deviceRepository;
    @Mock private DeviceHeartbeatService heartbeatService;
    @Mock private Mqtt5Publish publish;
    @Mock private MqttTopic topic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttSubscriber subscriber;

    @BeforeEach
    void setUp() {
        subscriber = new MqttSubscriber(new MqttProperties(), telemetryService, deviceRepository, heartbeatService, objectMapper);
    }

    @Test
    void dispatch_validJson_deserializesAndInvokesTelemetryService() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"productKey\":\"PK_TEST\",\"timestamp\":1719655200000,\"properties\":{\"temperature\":235.5}}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        subscriber.dispatch(publish);

        verify(telemetryService).process(argThat(msg ->
                "DK_TEST_001".equals(msg.getDeviceKey()) && "PK_TEST".equals(msg.getProductKey())));
    }

    @Test
    void dispatch_invalidJson_doesNotThrowAndSkipsTelemetryService() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatch(publish));

        verifyNoInteractions(telemetryService);
    }

    @Test
    void dispatch_telemetryServiceThrows_exceptionIsCaughtNotPropagated() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"productKey\":\"PK_TEST\",\"timestamp\":1719655200000,\"properties\":{\"temperature\":235.5}}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);
        doThrow(new RuntimeException("db down")).when(telemetryService).process(any());

        assertDoesNotThrow(() -> subscriber.dispatch(publish));
    }

    @Test
    void dispatchOnline_knownDevice_callsHeartbeat() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"status\":\"online\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        Device device = new Device();
        device.setId(42L);
        device.setDeviceKey("DK_TEST_001");
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0))
                .thenReturn(Optional.of(device));

        subscriber.dispatchOnline(publish);

        verify(heartbeatService).heartbeat(42L, "DK_TEST_001");
    }

    @Test
    void dispatchOnline_unknownDevice_doesNotCallHeartbeat() {
        String json = "{\"deviceKey\":\"DK_UNKNOWN\",\"status\":\"online\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_UNKNOWN", (short) 0))
                .thenReturn(Optional.empty());

        subscriber.dispatchOnline(publish);

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchOnline_invalidJson_doesNotThrow() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatchOnline(publish));

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchOffline_realDeviceKey_callsMarkOfflineNow() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"status\":\"offline\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        subscriber.dispatchOffline(publish);

        verify(heartbeatService).markOfflineNow("DK_TEST_001");
        verify(heartbeatService, never()).markAllOffline();
    }

    @Test
    void dispatchOffline_emulatorDeviceKey_callsMarkAllOffline() {
        String json = "{\"deviceKey\":\"emulator\",\"status\":\"offline\",\"timestamp\":1719655200000}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        subscriber.dispatchOffline(publish);

        verify(heartbeatService).markAllOffline();
        verify(heartbeatService, never()).markOfflineNow(any());
    }

    @Test
    void dispatchOffline_invalidJson_doesNotThrow() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatchOffline(publish));

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchStatus_knownDevice_callsHeartbeat() {
        String json = "{\"deviceKey\":\"DK_TEST_001\",\"productKey\":\"PK_TEST\",\"timestamp\":1719655200000,\"status\":\"running\",\"uptime\":120}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        Device device = new Device();
        device.setId(42L);
        device.setDeviceKey("DK_TEST_001");
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST_001", (short) 0))
                .thenReturn(Optional.of(device));

        subscriber.dispatchStatus(publish);

        verify(heartbeatService).heartbeat(42L, "DK_TEST_001");
    }

    @Test
    void dispatchStatus_unknownDevice_doesNotCallHeartbeat() {
        String json = "{\"deviceKey\":\"DK_UNKNOWN\",\"status\":\"running\",\"uptime\":120}";
        when(publish.getPayloadAsBytes()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_UNKNOWN", (short) 0))
                .thenReturn(Optional.empty());

        subscriber.dispatchStatus(publish);

        verifyNoInteractions(heartbeatService);
    }

    @Test
    void dispatchStatus_invalidJson_doesNotThrow() {
        when(publish.getPayloadAsBytes()).thenReturn("not json".getBytes(StandardCharsets.UTF_8));
        when(publish.getTopic()).thenReturn(topic);

        assertDoesNotThrow(() -> subscriber.dispatchStatus(publish));

        verifyNoInteractions(heartbeatService);
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.mqtt.MqttSubscriberTest"`
Expected: FAIL — compilation error, `constructor MqttSubscriber cannot be applied to given types` and `cannot find symbol: method dispatchOnline/dispatchOffline/dispatchStatus`

- [ ] **Step 5: Implement the subscriber changes**

Replace `src/main/java/com/spark/agent/mqtt/MqttSubscriber.java` in full:

```java
package com.spark.agent.mqtt;

import tools.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.spark.agent.config.MqttProperties;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.service.DeviceHeartbeatService;
import com.spark.agent.service.TelemetryService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class MqttSubscriber implements ApplicationRunner {

    private final MqttProperties props;
    private final TelemetryService telemetryService;
    private final DeviceRepository deviceRepository;
    private final DeviceHeartbeatService heartbeatService;
    private final ObjectMapper objectMapper;
    private Mqtt5AsyncClient client;
    private final Executor mqttExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public MqttSubscriber(MqttProperties props, TelemetryService telemetryService,
                           DeviceRepository deviceRepository, DeviceHeartbeatService heartbeatService,
                           ObjectMapper objectMapper) {
        this.props = props;
        this.telemetryService = telemetryService;
        this.deviceRepository = deviceRepository;
        this.heartbeatService = heartbeatService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initClient() {
        client = Mqtt5Client.builder()
                .identifier(props.getClientIdPrefix() + "-" + UUID.randomUUID().toString().replace("-", ""))
                .serverHost(props.getHost())
                .serverPort(props.getPort())
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(ctx -> subscribe())
                .addDisconnectedListener(ctx -> {
                    Throwable cause = ctx.getCause();
                    log.warn("[MQTT] Disconnected: {}", cause != null ? cause.getMessage() : "unknown");
                })
                .buildAsync();
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("[MQTT] Connecting to {}:{}", props.getHost(), props.getPort());
        try {
            client.connectWith()
                    .cleanStart(true)
                    .send()
                    .get();
            log.info("[MQTT] Connected successfully");
        } catch (Exception e) {
            log.error("[MQTT] Initial connection failed: {}", e.getMessage());
            throw new RuntimeException("MQTT initial connection failed, aborting startup", e);
        }
    }

    private void subscribe() {
        subscribeTo(props.getTopic(), this::handleMessage);
        subscribeTo(props.getOnlineTopic(), this::handleOnlineMessage);
        subscribeTo(props.getOfflineTopic(), this::handleOfflineMessage);
        subscribeTo(props.getStatusTopic(), this::handleStatusMessage);
    }

    private void subscribeTo(String topicFilter, java.util.function.Consumer<Mqtt5Publish> callback) {
        log.info("[MQTT] Subscribing to {}", topicFilter);
        client.subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .callback(callback::accept)
                .send()
                .thenAccept(ack -> log.info("[MQTT] Subscribed to {}: {}", topicFilter, ack.getReasonCodes()));
    }

    @PreDestroy
    public void destroy() {
        if (client != null) {
            client.disconnect();
            log.info("[MQTT] Client disconnected");
        }
    }

    private void handleMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatch(message));
    }

    private void handleOnlineMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatchOnline(message));
    }

    private void handleOfflineMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatchOffline(message));
    }

    private void handleStatusMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatchStatus(message));
    }

    void dispatch(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceTelemetryMessage msg = objectMapper.readValue(payload, DeviceTelemetryMessage.class);
            telemetryService.process(msg);
        } catch (Exception e) {
            log.error("[MQTT] Error processing message from {}: {}", message.getTopic(), e.getMessage());
        }
    }

    void dispatchOnline(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceConnectionEventMessage msg = objectMapper.readValue(payload, DeviceConnectionEventMessage.class);
            deviceRepository.findByDeviceKeyAndDeleted(msg.getDeviceKey(), (short) 0)
                    .ifPresentOrElse(
                            device -> heartbeatService.heartbeat(device.getId(), device.getDeviceKey()),
                            () -> log.warn("[MQTT] Online event for unknown device: {}", msg.getDeviceKey()));
        } catch (Exception e) {
            log.error("[MQTT] Error processing online event from {}: {}", message.getTopic(), e.getMessage());
        }
    }

    void dispatchOffline(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceConnectionEventMessage msg = objectMapper.readValue(payload, DeviceConnectionEventMessage.class);
            if ("emulator".equals(msg.getDeviceKey())) {
                heartbeatService.markAllOffline();
            } else {
                heartbeatService.markOfflineNow(msg.getDeviceKey());
            }
        } catch (Exception e) {
            log.error("[MQTT] Error processing offline event from {}: {}", message.getTopic(), e.getMessage());
        }
    }

    void dispatchStatus(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceStatusEventMessage msg = objectMapper.readValue(payload, DeviceStatusEventMessage.class);
            deviceRepository.findByDeviceKeyAndDeleted(msg.getDeviceKey(), (short) 0)
                    .ifPresentOrElse(
                            device -> heartbeatService.heartbeat(device.getId(), device.getDeviceKey()),
                            () -> log.warn("[MQTT] Status event for unknown device: {}", msg.getDeviceKey()));
        } catch (Exception e) {
            log.error("[MQTT] Error processing status event from {}: {}", message.getTopic(), e.getMessage());
        }
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.mqtt.MqttSubscriberTest"`
Expected: PASS — 12 tests completed, 0 failed

- [ ] **Step 7: Update `application.yaml`**

In `src/main/resources/application.yaml`, under the `# ── Custom: MQTT ──` section, change:

```yaml
mqtt:
  host: localhost
  port: 1883
  topic: "/sys/+/+/thing/event/property/post"
  client-id-prefix: spark-agent
```

to:

```yaml
mqtt:
  host: localhost
  port: 1883
  topic: "/sys/+/+/thing/event/property/post"
  online-topic: "device/online/+"
  offline-topic: "device/offline/+"       # also catches the emulator's crash LWT: device/offline/emulator
  status-topic: "/sys/+/+/thing/event/status/post"
  client-id-prefix: spark-agent
```

- [ ] **Step 8: Update `CLAUDE.md` configuration reference**

In `CLAUDE.md`, under `## Configuration Reference`, change:

```yaml
mqtt:
  host: localhost          # EMQX host
  port: 1883
  topic: "/sys/+/+/thing/event/property/post"
  client-id-prefix: spark-agent
```

to:

```yaml
mqtt:
  host: localhost          # EMQX host
  port: 1883
  topic: "/sys/+/+/thing/event/property/post"
  online-topic: "device/online/+"                     # emulator's explicit online events
  offline-topic: "device/offline/+"                    # explicit offline events + emulator crash LWT (device/offline/emulator)
  status-topic: "/sys/+/+/thing/event/status/post"     # periodic uptime heartbeat; deviceKey used to refresh online state, uptime value discarded
  client-id-prefix: spark-agent
```

Also add one line to the "Architecture" ASCII diagram's `MqttSubscriber` box description, changing:

```
MqttSubscriber          HiveMQ async client; reconnects automatically;
      │                     re-subscribes via addConnectedListener on each connect
```

to:

```
MqttSubscriber          HiveMQ async client; reconnects automatically;
      │                     re-subscribes via addConnectedListener on each connect;
      │                     also subscribes to device/online, device/offline (+ LWT), and
      │                     status/post for instant online/offline transitions
```

- [ ] **Step 9: Run the full test suite**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL, all tests pass (requires EMQX/Postgres/Redis/Kafka from `spark-ai-infra` running)

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/spark/agent/mqtt/DeviceConnectionEventMessage.java \
        src/main/java/com/spark/agent/mqtt/DeviceStatusEventMessage.java \
        src/main/java/com/spark/agent/config/MqttProperties.java \
        src/main/java/com/spark/agent/mqtt/MqttSubscriber.java \
        src/test/java/com/spark/agent/mqtt/MqttSubscriberTest.java \
        src/main/resources/application.yaml \
        CLAUDE.md
git commit -m "feat(mqtt): subscribe to device online/offline/status events for instant connectivity detection"
```
