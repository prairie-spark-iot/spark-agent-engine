# Transactional Outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DB writes and Kafka event publication atomically consistent for telemetry and alert data by routing both through a Transactional Outbox instead of firing Kafka directly inside `@Transactional` methods.

**Architecture:** `TelemetryService` and `AlertService` write an `OutboxMessage` row in the *same* DB transaction as their business row (device data / alert record). A new `OutboxRelayService`, polling on a fixed schedule, reads unpublished outbox rows, sends each to Kafka, and marks it published only after a confirmed send. DB commit/rollback and event publication become atomic; Kafka delivery becomes at-least-once via retry-until-published.

**Tech Stack:** Java 25, Spring Boot 4.1.0, Spring Data JPA (PostgreSQL, `ddl-auto: none`), Spring Kafka, Jackson 3 (`tools.jackson.databind`), Lombok, JUnit 5 + Mockito.

## Global Constraints

- `ddl-auto: none` — no Hibernate auto-DDL. Schema changes are tracked as standalone timestamped SQL files under `sql/` and applied by hand; there is no `postgres-init.sql` in this repo (see `sql/2026-07-01-device-data-index.sql` for the existing precedent).
- Jackson 3 package is `tools.jackson.databind.ObjectMapper` / `tools.jackson.core.*`, not `com.fasterxml.jackson.databind`. `JacksonException` (thrown by `readValue`/`readTree`/`writeValueAsString`) is unchecked (`extends RuntimeException`) in Jackson 3 — no checked-exception handling needed around these calls.
- All entities use snowflake IDs assigned client-side via `SnowflakeIdGenerator.nextId()` (autowired `@Component`, `@Value("${app.snowflake.machine-id:1}")`) — no DB auto-increment.
- Existing test convention: JUnit 5 `@ExtendWith(MockitoExtension.class)`, `@Mock` fields, manual `new Xxx(mock1, mock2, ...)` construction in `@BeforeEach` (no `@InjectMocks`), `import static org.mockito.Mockito.*` / `org.mockito.ArgumentMatchers.*` / `org.junit.jupiter.api.Assertions.*`.
- Run tests with `./gradlew test --tests "com.spark.agent.<package>.<ClassName>"` (single class) or `./gradlew build -x test` for a compile-only check between steps if a full test run is slow.
- Full spec: `docs/superpowers/specs/2026-07-02-transactional-outbox-design.md`.

---

### Task 1: Outbox persistence layer — entity, repository, SQL, factory

**Files:**
- Create: `src/main/java/com/spark/agent/entity/OutboxMessage.java`
- Create: `src/main/java/com/spark/agent/repository/OutboxMessageRepository.java`
- Create: `sql/2026-07-02-outbox-table.sql`
- Create: `src/main/java/com/spark/agent/service/OutboxMessageFactory.java`
- Test: `src/test/java/com/spark/agent/service/OutboxMessageFactoryTest.java`

**Interfaces:**
- Produces: `OutboxMessage` (getters/setters: `id`, `aggregateType`, `aggregateId`, `eventType`, `payload`, `createdAt`, `publishedAt` — all `Long`/`String`/`LocalDateTime`), `OutboxMessageRepository` (`saveAll(Iterable<OutboxMessage>)`, `save(OutboxMessage)` inherited from `JpaRepository`; `findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable)`; `markPublished(Long id, LocalDateTime publishedAt)`), `OutboxMessageFactory.build(String aggregateType, String aggregateId, String eventType, Object entity)` returning a fully-populated `OutboxMessage` (id/createdAt set, publishedAt null, payload = JSON of `entity`). Tasks 3, 4, 5 all depend on these exact names/signatures.

- [ ] **Step 1: Write the entity**

```java
package com.spark.agent.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "aiot_outbox")
public class OutboxMessage {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;
}
```

Does not extend `BaseEntity` — `deleted`/`tenantId`/`creator`/`updater` don't apply to an append-only relay log.

- [ ] **Step 2: Write the repository**

```java
package com.spark.agent.repository;

import com.spark.agent.entity.OutboxMessage;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    List<OutboxMessage> findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable pageable);

    @Modifying
    @Transactional
    @Query("UPDATE OutboxMessage o SET o.publishedAt = :publishedAt WHERE o.id = :id")
    void markPublished(Long id, LocalDateTime publishedAt);
}
```

`@Transactional` sits directly on the `@Modifying` query method — Spring Data JPA repository proxies apply transactional advice based on annotations on the interface method itself, so this works regardless of whether the caller (`OutboxRelayService`, Task 5) is transactional.

- [ ] **Step 3: Write the SQL migration file**

```sql
-- New table for the Transactional Outbox pattern: TelemetryService and AlertService
-- write outbox rows in the same DB transaction as their business writes, instead of
-- calling Kafka directly. OutboxRelayService polls unpublished rows and publishes them.
-- See docs/superpowers/specs/2026-07-02-transactional-outbox-design.md
--
-- Schema is managed manually in this project (spring.jpa.hibernate.ddl-auto: none,
-- no migration tooling) — this file is the tracked record of that manual change.
-- Apply manually to any environment before deploying the outbox-writing code.
CREATE TABLE aiot_outbox (
    id             bigint PRIMARY KEY,
    aggregate_type varchar(32)  NOT NULL,
    aggregate_id   varchar(64)  NOT NULL,
    event_type     varchar(64)  NOT NULL,
    payload        text         NOT NULL,
    created_at     timestamp without time zone NOT NULL,
    published_at   timestamp without time zone
);

CREATE INDEX idx_outbox_unpublished
  ON aiot_outbox (created_at)
  WHERE published_at IS NULL;
```

- [ ] **Step 4: Apply the migration to the local dev DB**

Run: `docker exec -i <postgres-container-name> psql -U root -d spark_ai < sql/2026-07-02-outbox-table.sql`

(Use `docker ps` to confirm the actual postgres container name if it's not `spark-postgres`.)

Expected: `CREATE TABLE` and `CREATE INDEX` printed, no errors. Verify with:
`docker exec <postgres-container-name> psql -U root -d spark_ai -c '\d aiot_outbox'` — should show all 7 columns.

- [ ] **Step 5: Write the failing test for `OutboxMessageFactory`**

```java
package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.OutboxMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxMessageFactoryTest {

    @Mock
    private SnowflakeIdGenerator idGenerator;

    private ObjectMapper objectMapper;
    private OutboxMessageFactory factory;

    record SamplePayload(String deviceKey, String identifier) {}

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        factory = new OutboxMessageFactory(idGenerator, objectMapper);
    }

    @Test
    void build_populatesAllFieldsAndSerializesPayload() {
        when(idGenerator.nextId()).thenReturn(555L);
        SamplePayload payload = new SamplePayload("DK_TEST_001", "temperature");

        OutboxMessage msg = factory.build("device_data", "123", "device.data", payload);

        assertEquals(555L, msg.getId());
        assertEquals("device_data", msg.getAggregateType());
        assertEquals("123", msg.getAggregateId());
        assertEquals("device.data", msg.getEventType());
        assertNull(msg.getPublishedAt());
        assertNotNull(msg.getCreatedAt());
        assertTrue(msg.getPayload().contains("DK_TEST_001"));
        assertTrue(msg.getPayload().contains("temperature"));
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.service.OutboxMessageFactoryTest"`
Expected: FAIL — compile error, `OutboxMessageFactory` does not exist yet.

- [ ] **Step 7: Write `OutboxMessageFactory`**

```java
package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.OutboxMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class OutboxMessageFactory {

    private final SnowflakeIdGenerator idGenerator;
    private final ObjectMapper objectMapper;

    public OutboxMessage build(String aggregateType, String aggregateId, String eventType, Object entity) {
        OutboxMessage msg = new OutboxMessage();
        msg.setId(idGenerator.nextId());
        msg.setAggregateType(aggregateType);
        msg.setAggregateId(aggregateId);
        msg.setEventType(eventType);
        msg.setPayload(objectMapper.writeValueAsString(entity));
        msg.setCreatedAt(LocalDateTime.now());
        return msg;
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.service.OutboxMessageFactoryTest"`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/main/java/com/spark/agent/entity/OutboxMessage.java \
        src/main/java/com/spark/agent/repository/OutboxMessageRepository.java \
        src/main/java/com/spark/agent/service/OutboxMessageFactory.java \
        src/test/java/com/spark/agent/service/OutboxMessageFactoryTest.java \
        sql/2026-07-02-outbox-table.sql
git commit -m "feat(outbox): add OutboxMessage entity, repository, and factory"
```

---

### Task 2: `KafkaProducerService.sendRaw` — pre-serialized send for the relay

**Files:**
- Modify: `src/main/java/com/spark/agent/kafka/KafkaProducerService.java`
- Test: `src/test/java/com/spark/agent/kafka/KafkaProducerServiceTest.java` (new file)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `KafkaProducerService.sendRaw(String topic, String key, String jsonPayload)` returning `CompletableFuture<SendResult<Object, Object>>`. Task 5 depends on this exact signature.

- [ ] **Step 1: Write the failing test**

```java
package com.spark.agent.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    private KafkaProducerService service;

    @BeforeEach
    void setUp() {
        service = new KafkaProducerService(kafkaTemplate, new ObjectMapper());
    }

    @Test
    void sendRaw_sendsExactPayloadWithNoReSerialization() {
        CompletableFuture<org.springframework.kafka.support.SendResult<Object, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send("iot.device.data", "DK_TEST_001", "{\"deviceKey\":\"DK_TEST_001\"}"))
                .thenReturn(future);

        var result = service.sendRaw("iot.device.data", "DK_TEST_001", "{\"deviceKey\":\"DK_TEST_001\"}");

        assertSame(future, result);
        verify(kafkaTemplate).send("iot.device.data", "DK_TEST_001", "{\"deviceKey\":\"DK_TEST_001\"}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.kafka.KafkaProducerServiceTest"`
Expected: FAIL — compile error, `sendRaw` does not exist yet.

- [ ] **Step 3: Add `sendRaw` to `KafkaProducerService`**

Modify `src/main/java/com/spark/agent/kafka/KafkaProducerService.java` — add this import alongside the existing ones:

```java
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
```

Add this method after `sendAlert`:

```java
    public CompletableFuture<SendResult<Object, Object>> sendRaw(String topic, String key, String jsonPayload) {
        return kafkaTemplate.send(topic, key, jsonPayload);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.kafka.KafkaProducerServiceTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/kafka/KafkaProducerService.java \
        src/test/java/com/spark/agent/kafka/KafkaProducerServiceTest.java
git commit -m "feat(outbox): add KafkaProducerService.sendRaw for pre-serialized relay sends"
```

---

### Task 3: Rewire `TelemetryService` to write outbox rows instead of calling Kafka directly

**Files:**
- Modify: `src/main/java/com/spark/agent/service/TelemetryService.java`
- Test: `src/test/java/com/spark/agent/service/TelemetryServiceTest.java` (new file — none exists today)

**Interfaces:**
- Consumes: `OutboxMessageRepository.saveAll(Iterable<OutboxMessage>)` (Task 1), `OutboxMessageFactory.build(String, String, String, Object)` (Task 1).
- Produces: no change to `TelemetryService.process(DeviceTelemetryMessage msg)` signature — `MqttSubscriber` (unmodified) keeps calling it as before.

- [ ] **Step 1: Write the failing tests**

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.TelemetryServiceTest"`
Expected: FAIL — compile error, `TelemetryService` constructor doesn't match (still takes `kafkaProducerService`, not `outboxMessageRepository`/`outboxMessageFactory`).

- [ ] **Step 3: Rewire `TelemetryService`**

Replace the full contents of `src/main/java/com/spark/agent/service/TelemetryService.java`:

```java
package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.mqtt.DeviceTelemetryMessage;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.OutboxMessageRepository;
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
    private final OutboxMessageRepository outboxMessageRepository;
    private final OutboxMessageFactory outboxMessageFactory;
    private final SnowflakeIdGenerator idGenerator;
    private final DeviceHeartbeatService heartbeatService;

    @Transactional
    public void process(DeviceTelemetryMessage msg) {
        Optional<Device> deviceOpt = deviceRepository.findByDeviceKeyAndDeleted(msg.getDeviceKey(), (short) 0);
        if (deviceOpt.isEmpty()) {
            log.warn("[Telemetry] Unknown device: {}", msg.getDeviceKey());
            return;
        }
        Device device = deviceOpt.get();
        LocalDateTime reportTime = toLocalDateTime(msg.getTimestamp());

        heartbeatService.heartbeat(device.getId(), device.getDeviceKey());

        List<DeviceData> rows = buildRows(msg, device.getId(), reportTime);
        deviceDataRepository.saveAll(rows);
        deviceDataRepository.flush();

        List<OutboxMessage> outboxRows = new ArrayList<>();
        for (DeviceData row : rows) {
            outboxRows.add(outboxMessageFactory.build("device_data", String.valueOf(row.getId()), "device.data", row));
            alertService.evaluate(row);
        }
        outboxMessageRepository.saveAll(outboxRows);

        log.debug("[Telemetry] Processed {} properties for {}", rows.size(), msg.getDeviceKey());
    }

    private List<DeviceData> buildRows(DeviceTelemetryMessage msg, Long deviceId, LocalDateTime reportTime) {
        List<DeviceData> rows = new ArrayList<>();
        Map<String, Object> properties = msg.getProperties();
        if (properties == null || properties.isEmpty()) {
            log.warn("[Telemetry] Message from {} has null/empty properties", msg.getDeviceKey());
            return rows;
        }
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
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
```

Changes from the original: `kafkaProducerService` field removed (its only use, `sendTelemetry(row)`, is removed from the loop); `outboxMessageRepository` and `outboxMessageFactory` added; the loop now builds one `OutboxMessage` per row and `saveAll`s them after the loop.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.TelemetryServiceTest"`
Expected: PASS (all 3 tests)

- [ ] **Step 5: Full build check (other callers of `TelemetryService`'s old constructor, if any, must still compile)**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL. (`TelemetryService` is only constructed by Spring via `@RequiredArgsConstructor` — no manual `new TelemetryService(...)` elsewhere in `src/main` to break.)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/spark/agent/service/TelemetryService.java \
        src/test/java/com/spark/agent/service/TelemetryServiceTest.java
git commit -m "feat(outbox): rewire TelemetryService to write outbox rows instead of direct Kafka send"
```

---

### Task 4: Rewire `AlertService` to write outbox rows instead of calling Kafka directly

**Files:**
- Modify: `src/main/java/com/spark/agent/service/AlertService.java`
- Modify: `src/test/java/com/spark/agent/service/AlertServiceTest.java` (existing file — replace `kafkaProducerService` mock/assertions with outbox equivalents)

**Interfaces:**
- Consumes: `OutboxMessageRepository.save(OutboxMessage)` (Task 1), `OutboxMessageFactory.build(String, String, String, Object)` (Task 1).
- Produces: no change to `AlertService.evaluate(DeviceData data)` signature — `TelemetryService` (Task 3) keeps calling it as before, now within `evaluate()`'s own `@Transactional` (joins the caller's transaction via default `REQUIRED` propagation).

- [ ] **Step 1: Update the failing test — replace Kafka mock/assertions with outbox equivalents**

Replace the full contents of `src/test/java/com/spark/agent/service/AlertServiceTest.java`:

```java
package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.AlertRule;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.AlertRuleRepository;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertServiceTest {

    @Mock
    private AlertRuleRepository alertRuleRepository;
    @Mock
    private AlertRecordRepository alertRecordRepository;
    @Mock
    private OutboxMessageRepository outboxMessageRepository;
    @Mock
    private OutboxMessageFactory outboxMessageFactory;
    @Mock
    private SnowflakeIdGenerator idGenerator;

    private AppProperties appProperties;
    private AlertService alertService;

    private DeviceData sampleData;
    private AlertRule sampleRule;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties();
        appProperties.setAlertDebounceMinutes(5);
        alertService = new AlertService(alertRuleRepository, alertRecordRepository,
                outboxMessageRepository, outboxMessageFactory, idGenerator, appProperties);

        sampleData = new DeviceData();
        sampleData.setDeviceId(1L);
        sampleData.setDeviceKey("DK_TEST_001");
        sampleData.setIdentifier("temperature");
        sampleData.setValueNum(new BigDecimal("150.5"));
        sampleData.setValue("150.5");
        sampleData.setReportTime(LocalDateTime.now());

        sampleRule = new AlertRule();
        sampleRule.setId(100L);
        sampleRule.setName("High Temperature");
        sampleRule.setOperator("gt");
        sampleRule.setThreshold("100");
        sampleRule.setLevel((short) 2);
    }

    @Test
    void evaluate_nullValueNum_doesNothing() {
        sampleData.setValueNum(null);
        alertService.evaluate(sampleData);
        verifyNoInteractions(alertRuleRepository);
    }

    @Test
    void evaluate_noMatchingRules_noAlert() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of());
        alertService.evaluate(sampleData);
        verify(alertRuleRepository).findActiveRules(1L, "temperature");
        verifyNoInteractions(alertRecordRepository);
    }

    @Test
    void evaluate_matchingRule_createsAlertAndOutboxRow() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(0L);
        when(idGenerator.nextId()).thenReturn(999L);
        when(outboxMessageFactory.build(eq("alert_record"), any(), eq("alert.triggered"), any(AlertRecord.class)))
                .thenReturn(new OutboxMessage());

        alertService.evaluate(sampleData);

        verify(alertRecordRepository).save(any(AlertRecord.class));
        verify(outboxMessageFactory).build(eq("alert_record"), any(), eq("alert.triggered"), any(AlertRecord.class));
        verify(outboxMessageRepository).save(any(OutboxMessage.class));
    }

    @Test
    void evaluate_debounced_skipsAlert() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(1L);

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
        verify(outboxMessageRepository, never()).save(any());
    }

    @Test
    void evaluate_valueBelowThreshold_noAlert() {
        sampleData.setValueNum(new BigDecimal("50"));
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    // ---- matches() operator tests via evaluate ----

    @Test
    void evaluate_gtOperator_triggersWhenAbove() {
        sampleData.setValueNum(new BigDecimal("101"));
        testOperatorTriggers("gt", "100");
    }

    @Test
    void evaluate_ltOperator_triggersWhenBelow() {
        sampleData.setValueNum(new BigDecimal("50"));
        testOperatorTriggers("lt", "100");
    }

    @Test
    void evaluate_gteOperator_triggersWhenEqual() {
        sampleData.setValueNum(new BigDecimal("100"));
        testOperatorTriggers("gte", "100");
    }

    @Test
    void evaluate_lteOperator_triggersWhenEqual() {
        sampleData.setValueNum(new BigDecimal("100"));
        testOperatorTriggers("lte", "100");
    }

    @Test
    void evaluate_eqOperator_triggersOnExactMatch() {
        sampleData.setValueNum(new BigDecimal("99.9"));
        testOperatorTriggers("eq", "99.9");
    }

    @Test
    void evaluate_eqOperator_doesNotTriggerOnNearMatch() {
        sampleData.setValueNum(new BigDecimal("99.9001"));
        sampleRule.setOperator("eq");
        sampleRule.setThreshold("99.9");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void evaluate_neOperator_triggersOnDifferent() {
        sampleData.setValueNum(new BigDecimal("50"));
        testOperatorTriggers("ne", "100");
    }

    @Test
    void evaluate_invalidThreshold_doesNotTrigger() {
        sampleRule.setThreshold("not-a-number");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void evaluate_invalidOperator_doesNotTrigger() {
        sampleRule.setOperator("invalid_op");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    private void testOperatorTriggers(String operator, String threshold) {
        sampleRule.setOperator(operator);
        sampleRule.setThreshold(threshold);
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(0L);
        when(idGenerator.nextId()).thenReturn(999L);
        when(outboxMessageFactory.build(eq("alert_record"), any(), eq("alert.triggered"), any(AlertRecord.class)))
                .thenReturn(new OutboxMessage());

        alertService.evaluate(sampleData);

        verify(alertRecordRepository).save(any(AlertRecord.class));
        verify(outboxMessageRepository).save(any(OutboxMessage.class));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: FAIL — compile error, `AlertService` constructor doesn't match (still takes `kafkaProducerService`, not `outboxMessageRepository`/`outboxMessageFactory`).

- [ ] **Step 3: Rewire `AlertService`**

In `src/main/java/com/spark/agent/service/AlertService.java`:

Replace the import block's Kafka/repository imports:

```java
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.repository.OutboxMessageRepository;
```

(remove `import com.spark.agent.kafka.KafkaProducerService;`)

Add `org.springframework.transaction.annotation.Transactional` to the imports.

Replace the field declarations:

```java
    private final AlertRuleRepository alertRuleRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final OutboxMessageRepository outboxMessageRepository;
    private final OutboxMessageFactory outboxMessageFactory;
    private final SnowflakeIdGenerator idGenerator;
    private final AppProperties appProperties;
```

Replace the `evaluate` method:

```java
    @Transactional
    public void evaluate(DeviceData data) {
        if (data.getValueNum() == null) return;

        List<AlertRule> rules = getActiveRules(data.getDeviceId(), data.getIdentifier());
        double value = data.getValueNum().doubleValue();

        for (AlertRule rule : rules) {
            if (!matches(rule, value)) continue;

            synchronized (debounceLock(data.getDeviceId(), rule.getId())) {
                if (isDebounced(data.getDeviceId(), rule.getId())) continue;

                AlertRecord record = buildRecord(data, rule, value);
                alertRecordRepository.save(record);
                outboxMessageRepository.save(
                        outboxMessageFactory.build("alert_record", String.valueOf(record.getId()), "alert.triggered", record));
                log.info("[Alert] Rule '{}' triggered for {} {}: {}", rule.getName(), data.getDeviceKey(), data.getIdentifier(), value);
            }
        }
    }
```

Everything else in the file (`getActiveRules`, `matches`, `isDebounced`, `buildRecord`, the caches) is unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: PASS (all tests)

- [ ] **Step 5: Full build check**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/spark/agent/service/AlertService.java \
        src/test/java/com/spark/agent/service/AlertServiceTest.java
git commit -m "feat(outbox): rewire AlertService to write outbox rows instead of direct Kafka send"
```

---

### Task 5: `OutboxRelayService` — scheduled poll, publish, mark-published

**Files:**
- Create: `src/main/java/com/spark/agent/service/OutboxRelayService.java`
- Test: `src/test/java/com/spark/agent/service/OutboxRelayServiceTest.java`
- Modify: `src/main/java/com/spark/agent/config/AppProperties.java`
- Modify: `src/main/resources/application.yaml`
- Modify: `src/main/java/com/spark/agent/SparkAgentEngineApplication.java`

**Interfaces:**
- Consumes: `OutboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable)` and `.markPublished(Long, LocalDateTime)` (Task 1); `KafkaProducerService.sendRaw(String, String, String)` (Task 2); `AppProperties.getOutboxRelayBatchSize()` / `getOutboxRelayIntervalMs()` (this task).
- Produces: `OutboxRelayService.relay()` — no other task depends on this; it's the terminal consumer of the pipeline.

- [ ] **Step 1: Add config properties**

In `src/main/java/com/spark/agent/config/AppProperties.java`, add two fields:

```java
    private int outboxRelayIntervalMs = 2000;
    private int outboxRelayBatchSize = 100;
```

In `src/main/resources/application.yaml`, under the existing `app:` section (after `diagnosis-confidence-threshold: 80`):

```yaml
  # outbox relay — polls aiot_outbox for unpublished rows and sends them to Kafka
  outbox-relay-interval-ms: 2000
  outbox-relay-batch-size: 100
```

- [ ] **Step 2: Enable scheduling**

In `src/main/java/com/spark/agent/SparkAgentEngineApplication.java`, add the import and annotation:

```java
package com.spark.agent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SparkAgentEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkAgentEngineApplication.class, args);
	}

}
```

- [ ] **Step 3: Write the failing tests**

```java
package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock
    private OutboxMessageRepository outboxMessageRepository;
    @Mock
    private KafkaProducerService kafkaProducerService;

    private ObjectMapper objectMapper;
    private AppProperties appProperties;
    private OutboxRelayService relayService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        appProperties = new AppProperties();
        appProperties.setOutboxRelayBatchSize(100);
        relayService = new OutboxRelayService(outboxMessageRepository, kafkaProducerService, objectMapper, appProperties);
    }

    private OutboxMessage outboxMessage(long id, String eventType, String deviceKey) {
        OutboxMessage msg = new OutboxMessage();
        msg.setId(id);
        msg.setAggregateType(eventType.equals("device.data") ? "device_data" : "alert_record");
        msg.setAggregateId(String.valueOf(id));
        msg.setEventType(eventType);
        msg.setPayload("{\"deviceKey\":\"" + deviceKey + "\"}");
        msg.setCreatedAt(LocalDateTime.now());
        return msg;
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<SendResult<Object, Object>> completedSend() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }

    @Test
    void relay_emptyBatch_doesNothing() {
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of());

        relayService.relay();

        verifyNoInteractions(kafkaProducerService);
        verify(outboxMessageRepository, never()).markPublished(any(), any());
    }

    @Test
    void relay_successfulSend_marksPublished() {
        OutboxMessage msg = outboxMessage(1L, "device.data", "DK_TEST_001");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));
        when(kafkaProducerService.sendRaw("iot.device.data", "DK_TEST_001", msg.getPayload()))
                .thenReturn(completedSend());

        relayService.relay();

        verify(kafkaProducerService).sendRaw("iot.device.data", "DK_TEST_001", msg.getPayload());
        verify(outboxMessageRepository).markPublished(eq(1L), any(LocalDateTime.class));
    }

    @Test
    void relay_alertEventType_resolvesAlertTopic() {
        OutboxMessage msg = outboxMessage(2L, "alert.triggered", "DK_TEST_002");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));
        when(kafkaProducerService.sendRaw("iot.alert.triggered", "DK_TEST_002", msg.getPayload()))
                .thenReturn(completedSend());

        relayService.relay();

        verify(kafkaProducerService).sendRaw("iot.alert.triggered", "DK_TEST_002", msg.getPayload());
        verify(outboxMessageRepository).markPublished(eq(2L), any(LocalDateTime.class));
    }

    @Test
    void relay_sendFailure_leavesRowUnpublishedAndContinuesBatch() {
        OutboxMessage failing = outboxMessage(3L, "device.data", "DK_TEST_003");
        OutboxMessage succeeding = outboxMessage(4L, "device.data", "DK_TEST_004");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(failing, succeeding));

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException("kafka unreachable", new RuntimeException()));
        when(kafkaProducerService.sendRaw("iot.device.data", "DK_TEST_003", failing.getPayload()))
                .thenReturn(failedFuture);
        when(kafkaProducerService.sendRaw("iot.device.data", "DK_TEST_004", succeeding.getPayload()))
                .thenReturn(completedSend());

        relayService.relay();

        verify(outboxMessageRepository, never()).markPublished(eq(3L), any());
        verify(outboxMessageRepository).markPublished(eq(4L), any(LocalDateTime.class));
    }

    @Test
    void relay_unknownEventType_skipsRowWithoutThrowing() {
        OutboxMessage msg = outboxMessage(5L, "unknown.type", "DK_TEST_005");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));

        assertDoesNotThrow(() -> relayService.relay());

        verifyNoInteractions(kafkaProducerService);
        verify(outboxMessageRepository, never()).markPublished(any(), any());
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.OutboxRelayServiceTest"`
Expected: FAIL — compile error, `OutboxRelayService` does not exist yet.

- [ ] **Step 5: Write `OutboxRelayService`**

```java
package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxRelayService {

    private static final Map<String, String> EVENT_TYPE_TOPICS = Map.of(
            "device.data", "iot.device.data",
            "alert.triggered", "iot.alert.triggered"
    );

    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;

    @Scheduled(fixedDelayString = "${app.outbox-relay-interval-ms:2000}")
    public void relay() {
        List<OutboxMessage> batch = outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(
                PageRequest.of(0, appProperties.getOutboxRelayBatchSize()));

        for (OutboxMessage msg : batch) {
            try {
                String topic = EVENT_TYPE_TOPICS.get(msg.getEventType());
                if (topic == null) {
                    log.error("[OutboxRelay] Unknown eventType '{}' for outbox id={}, skipping", msg.getEventType(), msg.getId());
                    continue;
                }
                String key = extractDeviceKey(msg.getPayload());
                kafkaProducerService.sendRaw(topic, key, msg.getPayload()).get(5, TimeUnit.SECONDS);
                outboxMessageRepository.markPublished(msg.getId(), LocalDateTime.now());
            } catch (Exception e) {
                log.warn("[OutboxRelay] Failed to publish outbox id={}, will retry next cycle: {}", msg.getId(), e.getMessage());
            }
        }
    }

    private String extractDeviceKey(String payloadJson) {
        return objectMapper.readTree(payloadJson).get("deviceKey").asText();
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.OutboxRelayServiceTest"`
Expected: PASS (all 5 tests)

- [ ] **Step 7: Full build and test suite**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL, all tests (existing + new) pass.

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/spark/agent/service/OutboxRelayService.java \
        src/test/java/com/spark/agent/service/OutboxRelayServiceTest.java \
        src/main/java/com/spark/agent/config/AppProperties.java \
        src/main/resources/application.yaml \
        src/main/java/com/spark/agent/SparkAgentEngineApplication.java
git commit -m "feat(outbox): add OutboxRelayService scheduled publisher"
```

- [ ] **Step 9: Manual end-to-end verification**

With the docker stack running (EMQX, Postgres, Kafka, Redis — per `CLAUDE.md`'s Infrastructure table) and the app started (`./gradlew bootRun`):

1. Publish an MQTT telemetry message to `/sys/PK_TEST/DK_TEST_001/thing/event/property/post` (use `mosquitto_pub` or the app's existing test tooling) with a JSON body matching `DeviceTelemetryMessage`'s shape (see `CLAUDE.md`'s MQTT Message Format section) for a `deviceKey` that exists in `aiot_device`.
2. Immediately check: `docker exec <postgres-container> psql -U root -d spark_ai -c "SELECT id, event_type, published_at FROM aiot_outbox ORDER BY created_at DESC LIMIT 5;"` — expect new row(s) with `published_at` NULL.
3. Wait ~2–3 seconds, re-run the same query — expect `published_at` now set.
4. Confirm the message actually landed on Kafka: `docker exec spark-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot.device.data --from-beginning --max-messages 1` (or use the offset-check command from `CLAUDE.md`).
5. Regression check: confirm `GET /api/device/DK_TEST_001/latest` still returns the new value (unrelated to Kafka, but confirms the DB-write half of `TelemetryService.process()` still works).

If any step fails, do not mark this task complete — report what was observed instead.

---

## Post-Plan Note

This plan covers tasks 001–004 from the original P0 list (transactional outbox infrastructure, `TelemetryService`/`AlertService` integration, relay delivery). Tasks 005–008 (REST API validation, Bucket4j rate limiting, API key auth, `DiagnosisAgentService` transaction/idempotency/circuit-breaker work) are separate, independent subsystems per the brainstorming scope decomposition and are not covered here — each needs its own spec and plan.
