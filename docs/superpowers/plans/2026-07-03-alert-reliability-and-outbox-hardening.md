# Alert Reliability & Outbox Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the reliability/performance gaps found in a backend-architect review of `spark-agent-engine`'s alert-evaluation and outbox paths, without touching anything already known-good (Kafka idempotence config, HikariCP sizing, MQTT reconnect, Redis heartbeat, `device_data` composite index).

**Architecture:** No new subsystems. Each task is a small, targeted change to existing files: two SQL index migrations (manual, since `ddl-auto: none`), a swap of an in-JVM lock for a Postgres session-scoped advisory lock (`pg_advisory_xact_lock`) inside the existing `@Transactional` boundary, a new `@Scheduled` purge method alongside the existing outbox relay, a cache-TTL jitter tweak, and removal of dead code + a doc fix.

**Tech Stack:** Java 25, Spring Boot 4.1.0, Spring Data JPA, PostgreSQL, JUnit 5 + Mockito, `@SpringBootTest` integration tests against the local dev Postgres (no Testcontainers in this repo).

## Global Constraints

- Use `./gradlew`, never system gradle. `./gradlew test --tests "com.spark.agent.Xxx"` for a single class.
- `spring.jpa.hibernate.ddl-auto: none` — every schema change is a manual SQL file under `sql/`, dated `YYYY-MM-DD-<name>.sql`, applied by hand to the running dev Postgres (`docker exec spark-postgres psql -U root -d spark_ai -c "..."`). Follow the exact comment style already used in `sql/2026-07-01-device-data-index.sql` and `sql/2026-07-02-outbox-table.sql` (explain what the index is for, which code path depends on it, and that it must be applied manually to any new environment).
- Jackson types are `tools.jackson.databind.ObjectMapper` / `tools.jackson.core.*` (Jackson 3.x package rename), not `com.fasterxml.jackson.databind`. Annotations stay at `com.fasterxml.jackson.annotation`.
- `KafkaTemplate<Object, Object>`, not `<String, String>`.
- Integration tests that need a real DB connection follow the pattern in `src/test/java/com/spark/agent/repository/DeviceDataRepositoryTest.java`: `@SpringBootTest`, `@Autowired` repositories, manual insert in the test, cleanup in `@AfterEach` via `deleteAllByIdInBatch`/`deleteById`.
- Do not reintroduce direct `KafkaProducerService` calls from `@Transactional` business methods — all publishing goes through the outbox (`OutboxMessageFactory` → `OutboxMessageRepository` → `OutboxRelayService`).
- Out of scope for this plan (explicitly deferred, not forgotten): restructuring `AlertService`/`TelemetryService` to batch rule lookups once per device instead of once per property row. The reviewer flagged this as low urgency at current scale since `rulesCache` already makes repeat lookups cheap; revisit only if profiling shows it matters.

---

### Task 1: Index the alert-debounce hot path

**Files:**
- Create: `sql/2026-07-03-alert-record-debounce-index.sql`

**Interfaces:** None — pure infra change, no application code touches this task.

- [ ] **Step 1: Write the migration file**

```sql
-- Speeds up AlertRecordRepository.countRecentUnhandled, called once per
-- matching alert rule on every MQTT telemetry message (the debounce check
-- on the hot ingest path — see AlertService.evaluate() / isDebounced()).
-- Without it, this query falls back to a sequential scan of
-- aiot_alert_record filtered by device_id/rule_id/handle_status/deleted/
-- trigger_time as the table grows.
--
-- Schema is managed manually in this project (spring.jpa.hibernate.ddl-auto: none,
-- no migration tooling) — this file is the tracked record of that manual change.
-- Apply manually to any environment before relying on alert debounce at scale.
CREATE INDEX CONCURRENTLY idx_alert_record_debounce
  ON aiot_alert_record (device_id, rule_id, trigger_time DESC)
  WHERE handle_status = 0 AND deleted = 0;
```

- [ ] **Step 2: Apply it to the local dev Postgres**

Run: `docker exec spark-postgres psql -U root -d spark_ai -f -  < sql/2026-07-03-alert-record-debounce-index.sql`
Expected: `CREATE INDEX`

- [ ] **Step 3: Verify the query plan uses the index**

Run:
```bash
docker exec spark-postgres psql -U root -d spark_ai -c "EXPLAIN ANALYZE SELECT COUNT(*) FROM aiot_alert_record WHERE device_id = 1 AND rule_id = 1 AND handle_status = 0 AND deleted = 0 AND trigger_time >= now() - interval '5 minutes';"
```
Expected: plan mentions `idx_alert_record_debounce` (Index Scan / Index Only Scan / Bitmap Index Scan), not `Seq Scan on aiot_alert_record`.

- [ ] **Step 4: Commit**

```bash
git add sql/2026-07-03-alert-record-debounce-index.sql
git commit -m "perf(alert): add composite index for debounce hot-path query"
```

---

### Task 2: Replace the in-JVM debounce lock with a DB-level advisory lock

**Context:** `AlertService.evaluate()` currently does check-then-insert (`countRecentUnhandled` → `save`) guarded only by a `synchronized` block keyed off an in-JVM `ConcurrentHashMap<String, Object>` (`debounceLocks`). This only serializes threads within one JVM; if this service ever runs as more than one instance, two instances can both pass the count check and double-insert an alert. The map is also never evicted. Fix both by acquiring a Postgres transaction-scoped advisory lock (`pg_advisory_xact_lock`) inside the existing `@Transactional` boundary — it's released automatically at commit/rollback, serializes across processes and threads, and needs no new table or index. This removes the need for `debounceLocks` entirely.

**Files:**
- Modify: `src/main/java/com/spark/agent/service/AlertService.java`
- Modify: `src/test/java/com/spark/agent/service/AlertServiceTest.java`

**Interfaces:**
- Consumes: `jakarta.persistence.EntityManager` (new constructor dependency, Spring-provided bean).
- Produces: `AlertService` constructor signature becomes `AlertService(AlertRuleRepository, AlertRecordRepository, OutboxMessageRepository, OutboxMessageFactory, SnowflakeIdGenerator, AppProperties, EntityManager)` — Task 3's integration test and any other direct instantiation must match this.

- [ ] **Step 1: Update `AlertServiceTest` to mock `EntityManager` and the new constructor arg**

In `src/test/java/com/spark/agent/service/AlertServiceTest.java`, add imports and mocks:

```java
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
```

```java
    @Mock
    private EntityManager entityManager;
    @Mock
    private Query nativeQuery;
```

In `setUp()`, replace the `alertService = new AlertService(...)` line with:

```java
        lenient().when(entityManager.createNativeQuery(anyString())).thenReturn(nativeQuery);
        lenient().when(nativeQuery.setParameter(anyInt(), any())).thenReturn(nativeQuery);
        lenient().when(nativeQuery.getSingleResult()).thenReturn(null);

        alertService = new AlertService(alertRuleRepository, alertRecordRepository,
                outboxMessageRepository, outboxMessageFactory, idGenerator, appProperties, entityManager);
```

Add `import static org.mockito.Mockito.lenient;` and confirm `anyString`/`anyInt`/`any` are already covered by the existing `import static org.mockito.ArgumentMatchers.*;`.

- [ ] **Step 2: Run the tests to confirm they fail on the constructor mismatch**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: compile error / FAIL — `AlertService(AlertRuleRepository, ..., AppProperties)` constructor doesn't accept 7 args yet.

- [ ] **Step 3: Implement the advisory-lock swap in `AlertService`**

In `src/main/java/com/spark/agent/service/AlertService.java`:

Add import:
```java
import jakarta.persistence.EntityManager;
```

Remove these two members (lines 29-33 in the current file):
```java
    private final Map<String, Object> debounceLocks = new ConcurrentHashMap<>();

    private Object debounceLock(Long deviceId, Long ruleId) {
        return debounceLocks.computeIfAbsent(deviceId + ":" + ruleId, k -> new Object());
    }
```

Add `entityManager` to the constructor-injected field list (with the other `private final` fields, alongside `appProperties`):
```java
    private final EntityManager entityManager;
```

Replace the `evaluate()` loop body (currently lines 81-93):
```java
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
```
with:
```java
        for (AlertRule rule : rules) {
            if (!matches(rule, value)) continue;

            acquireDebounceLock(data.getDeviceId(), rule.getId());
            if (isDebounced(data.getDeviceId(), rule.getId())) continue;

            AlertRecord record = buildRecord(data, rule, value);
            alertRecordRepository.save(record);
            outboxMessageRepository.save(
                    outboxMessageFactory.build("alert_record", String.valueOf(record.getId()), "alert.triggered", record));
            log.info("[Alert] Rule '{}' triggered for {} {}: {}", rule.getName(), data.getDeviceKey(), data.getIdentifier(), value);
        }
```

Add a new private method near `isDebounced`:
```java
    /**
     * Session-scoped advisory lock keyed on (deviceId, ruleId), held for the rest of this
     * @Transactional method and released automatically at commit/rollback. Serializes the
     * debounce check-then-insert across threads AND across multiple app instances, unlike
     * the in-JVM lock this replaces. hashtextextended collisions only cause unrelated
     * device/rule pairs to serialize against each other, never a correctness issue.
     */
    private void acquireDebounceLock(Long deviceId, Long ruleId) {
        entityManager.createNativeQuery(
                        "SELECT pg_advisory_xact_lock(hashtextextended(CONCAT(CAST(?1 AS text), ':', CAST(?2 AS text)), 0))")
                .setParameter(1, deviceId)
                .setParameter(2, ruleId)
                .getSingleResult();
    }
```

Remove the now-unused `Map`/`ConcurrentHashMap` imports if nothing else in the file uses them (check: `rulesCache` still uses `Map` and `ConcurrentHashMap` — keep those imports, only the `debounceLocks` field/method are gone).

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: PASS, all existing tests green with no behavior change (mocked `EntityManager` makes `acquireDebounceLock` a no-op in unit tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/service/AlertService.java src/test/java/com/spark/agent/service/AlertServiceTest.java
git commit -m "fix(alert): replace in-JVM debounce lock with DB-level advisory lock"
```

---

### Task 3: Prove the advisory lock closes the race, with a real-DB concurrency test

**Context:** Task 2 fixed the race with a DB-level lock, but nothing exercises it under actual concurrent load against a real database. This also gives the debounce logic its first concurrency test.

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/AlertRecordRepository.java`
- Create: `src/test/java/com/spark/agent/service/AlertServiceConcurrencyTest.java`

**Interfaces:**
- Consumes: `AlertService.evaluate(DeviceData)` (unchanged signature), `AlertRuleRepository`, `DeviceRepository`, `SnowflakeIdGenerator` — all existing Spring beans.
- Produces: `AlertRecordRepository.findByRuleIdAndDeviceId(Long ruleId, Long deviceId): List<AlertRecord>` — a small new query method, reusable by any future test or diagnostic needing "alerts for this rule+device".

- [ ] **Step 1: Add the verification query method**

In `src/main/java/com/spark/agent/repository/AlertRecordRepository.java`, add:
```java
    List<AlertRecord> findByRuleIdAndDeviceId(Long ruleId, Long deviceId);
```

- [ ] **Step 2: Write the failing concurrency test**

Create `src/test/java/com/spark/agent/service/AlertServiceConcurrencyTest.java`:

```java
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
```

- [ ] **Step 3: Run it and confirm it currently passes (sanity check the test itself is well-formed)**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceConcurrencyTest"`
Expected: PASS. (This proves Task 2's fix works — if you want to see it *fail* first to validate the test catches the bug, temporarily revert the `acquireDebounceLock` call in `AlertService.evaluate()` to a no-op, rerun, observe `created.size()` > 1, then restore the fix.)

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/spark/agent/repository/AlertRecordRepository.java src/test/java/com/spark/agent/service/AlertServiceConcurrencyTest.java
git commit -m "test(alert): add concurrency test proving advisory lock prevents duplicate alerts"
```

---

### Task 4: Purge published outbox rows on a schedule

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/OutboxMessageRepository.java`
- Modify: `src/main/java/com/spark/agent/service/OutboxRelayService.java`
- Modify: `src/main/java/com/spark/agent/config/AppProperties.java`
- Modify: `src/main/resources/application.yaml`
- Modify: `src/test/java/com/spark/agent/service/OutboxRelayServiceTest.java`

**Interfaces:**
- Consumes: `AppProperties.getOutboxPurgeRetentionDays(): int` (new), `AppProperties.getOutboxPurgeCron(): String` (new).
- Produces: `OutboxMessageRepository.deletePublishedBefore(LocalDateTime cutoff): int`, `OutboxRelayService.purge(): void`.

- [ ] **Step 1: Add the delete query to the repository**

In `src/main/java/com/spark/agent/repository/OutboxMessageRepository.java`, add:

```java
    @Modifying
    @Transactional
    @Query("DELETE FROM OutboxMessage o WHERE o.publishedAt IS NOT NULL AND o.publishedAt < :cutoff")
    int deletePublishedBefore(LocalDateTime cutoff);
```

(`@Modifying`, `@Transactional`, and `LocalDateTime` are already imported in this file.)

- [ ] **Step 2: Add config properties**

In `src/main/java/com/spark/agent/config/AppProperties.java`, add after `outboxRelayBatchSize`:

```java
    private int outboxPurgeRetentionDays = 7;
    private String outboxPurgeCron = "0 0 3 * * *";
```

In `src/main/resources/application.yaml`, after the `outbox-relay-batch-size: 100` line (currently line 152), add:

```yaml
  # outbox purge — deletes published aiot_outbox rows older than this many days
  outbox-purge-retention-days: 7
  outbox-purge-cron: "0 0 3 * * *"   # daily at 03:00
```

- [ ] **Step 3: Write the failing test for `purge()`**

In `src/test/java/com/spark/agent/service/OutboxRelayServiceTest.java`, add:

```java
    @Test
    void purge_deletesPublishedRowsOlderThanRetention() {
        appProperties.setOutboxPurgeRetentionDays(7);
        when(outboxMessageRepository.deletePublishedBefore(any(LocalDateTime.class))).thenReturn(3);

        relayService.purge();

        verify(outboxMessageRepository).deletePublishedBefore(any(LocalDateTime.class));
    }
```

- [ ] **Step 4: Run to confirm it fails**

Run: `./gradlew test --tests "com.spark.agent.service.OutboxRelayServiceTest"`
Expected: FAIL — `OutboxRelayService` has no `purge()` method / `OutboxMessageRepository` mock has no `deletePublishedBefore` stub target yet.

- [ ] **Step 5: Implement `purge()`**

In `src/main/java/com/spark/agent/service/OutboxRelayService.java`, add after `relay()`:

```java
    @Scheduled(cron = "${app.outbox-purge-cron:0 0 3 * * *}")
    public void purge() {
        LocalDateTime cutoff = LocalDateTime.now().minusDays(appProperties.getOutboxPurgeRetentionDays());
        int deleted = outboxMessageRepository.deletePublishedBefore(cutoff);
        if (deleted > 0) {
            log.info("[OutboxRelay] Purged {} published outbox rows older than {}", deleted, cutoff);
        }
    }
```

- [ ] **Step 6: Run to confirm it passes**

Run: `./gradlew test --tests "com.spark.agent.service.OutboxRelayServiceTest"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/repository/OutboxMessageRepository.java src/main/java/com/spark/agent/service/OutboxRelayService.java src/main/java/com/spark/agent/config/AppProperties.java src/main/resources/application.yaml src/test/java/com/spark/agent/service/OutboxRelayServiceTest.java
git commit -m "feat(outbox): purge published rows on a daily schedule"
```

---

### Task 5: Index the diagnosis-retry widen-window query

**Files:**
- Create: `sql/2026-07-03-device-data-widen-window-index.sql`

**Interfaces:** None.

- [ ] **Step 1: Write the migration file**

```sql
-- Speeds up DeviceDataRepository.findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc,
-- used by the AI diagnosis retry path to widen telemetry context beyond the
-- latest-per-identifier snapshot when diagnosis confidence is below
-- app.diagnosis-retry-confidence-threshold.
-- This query has no identifier predicate, so it can't use
-- idx_device_data_key_identifier_time (sql/2026-07-01-device-data-index.sql)
-- efficiently — without a dedicated index it falls back to a sequential scan
-- filtered by device_key/deleted/report_time.
--
-- Schema is managed manually in this project (spring.jpa.hibernate.ddl-auto: none,
-- no migration tooling) — this file is the tracked record of that manual change.
-- Apply manually to any environment before relying on the diagnosis retry path at scale.
CREATE INDEX CONCURRENTLY idx_device_data_key_time
  ON aiot_device_data (device_key, report_time DESC)
  WHERE deleted = 0;
```

- [ ] **Step 2: Apply it to the local dev Postgres**

Run: `docker exec spark-postgres psql -U root -d spark_ai -f - < sql/2026-07-03-device-data-widen-window-index.sql`
Expected: `CREATE INDEX`

- [ ] **Step 3: Verify the query plan uses the index**

Run:
```bash
docker exec spark-postgres psql -U root -d spark_ai -c "EXPLAIN ANALYZE SELECT * FROM aiot_device_data WHERE device_key = 'DK_INJ_001' AND deleted = 0 AND report_time >= now() - interval '120 minutes' ORDER BY report_time DESC;"
```
Expected: plan mentions `idx_device_data_key_time`, not `Seq Scan on aiot_device_data`.

- [ ] **Step 4: Commit**

```bash
git add sql/2026-07-03-device-data-widen-window-index.sql
git commit -m "perf(device-data): add index for diagnosis-retry widen-window query"
```

---

### Task 6: Add jitter to the alert-rules cache TTL

**Files:**
- Modify: `src/main/java/com/spark/agent/service/AlertService.java`
- Modify: `src/test/java/com/spark/agent/service/AlertServiceTest.java`

**Interfaces:** None new — internal behavior change only.

- [ ] **Step 1: Write the failing test**

In `src/test/java/com/spark/agent/service/AlertServiceTest.java`, add:

```java
    @Test
    void evaluate_secondCallWithinTtl_reusesCachedRules() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(1L); // debounced both times — keeps this test focused on caching, not insert side-effects

        alertService.evaluate(sampleData);
        alertService.evaluate(sampleData);

        verify(alertRuleRepository, times(1)).findActiveRules(1L, "temperature");
    }
```

- [ ] **Step 2: Run to confirm it currently passes (cache already exists) — this establishes a baseline before the jitter change**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: PASS (caching already works; this step just proves the test is valid before we touch the TTL code).

- [ ] **Step 3: Add jitter to the cache TTL**

In `src/main/java/com/spark/agent/service/AlertService.java`, add import:
```java
import java.util.concurrent.ThreadLocalRandom;
```

Change:
```java
    private static final long RULES_CACHE_TTL_MS = 5_000;
    private static final int RULES_CACHE_MAX_SIZE = 10_000;
```
to:
```java
    private static final long RULES_CACHE_TTL_MS = 5_000;
    private static final long RULES_CACHE_JITTER_MS = 1_000;
    private static final int RULES_CACHE_MAX_SIZE = 10_000;
```

Change the cache-put line inside `getActiveRules`:
```java
        List<AlertRule> rules = alertRuleRepository.findActiveRules(deviceId, identifier);
        rulesCache.put(key, new CacheEntry<>(rules, System.currentTimeMillis() + RULES_CACHE_TTL_MS));
```
to:
```java
        List<AlertRule> rules = alertRuleRepository.findActiveRules(deviceId, identifier);
        long ttl = RULES_CACHE_TTL_MS + ThreadLocalRandom.current().nextLong(RULES_CACHE_JITTER_MS);
        rulesCache.put(key, new CacheEntry<>(rules, System.currentTimeMillis() + ttl));
```

- [ ] **Step 4: Run to confirm the test still passes**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: PASS — jitter only adds up to 1s of extra life to the cache entry, doesn't affect same-millisecond re-reads.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/service/AlertService.java src/test/java/com/spark/agent/service/AlertServiceTest.java
git commit -m "perf(alert): add jitter to rules-cache TTL to avoid synchronized expiry"
```

---

### Task 7: Remove dead Kafka direct-send methods and fix stale docs

**Context:** `KafkaProducerService.sendTelemetry()` and `sendAlert()` are unreferenced now that `TelemetryService` and `AlertService` publish via the outbox (`OutboxMessageFactory` → `OutboxMessageRepository` → `OutboxRelayService.relay()` → `kafkaProducerService.sendRaw(...)`). Confirmed via `grep -rn "sendTelemetry\|sendAlert" src/` — no callers. Removing them also removes the now-otherwise-unused `objectMapper` field and `deviceDataTopic`/`alertTopic` `@Value` fields from this class. `CLAUDE.md`'s architecture diagram and one "Key Design Decisions" note still describe the old direct-send flow and a stale "phase 2" framing (the outbox pattern is already implemented, not a future phase — separately, "Phase 2 — AI diagnosis" in the Future Phases section is *also* already implemented via `DiagnosisAgentService`/`DiagnosisPromptBuilder`, but fixing that section's phase numbering is out of scope here; only fix the outbox-related staleness this task's cleanup surfaces).

**Files:**
- Modify: `src/main/java/com/spark/agent/kafka/KafkaProducerService.java`
- Modify: `src/test/java/com/spark/agent/kafka/KafkaProducerServiceTest.java`
- Modify: `CLAUDE.md`

**Interfaces:**
- Produces: `KafkaProducerService` constructor becomes `KafkaProducerService(KafkaTemplate<Object, Object>)` — one arg, down from two.

- [ ] **Step 1: Update the test to match the smaller constructor**

In `src/test/java/com/spark/agent/kafka/KafkaProducerServiceTest.java`, remove the `ObjectMapper` import and change:
```java
        service = new KafkaProducerService(kafkaTemplate, new ObjectMapper());
```
to:
```java
        service = new KafkaProducerService(kafkaTemplate);
```

- [ ] **Step 2: Run to confirm it fails on the constructor mismatch**

Run: `./gradlew test --tests "com.spark.agent.kafka.KafkaProducerServiceTest"`
Expected: compile error — `KafkaProducerService(KafkaTemplate, ObjectMapper)` doesn't exist for a 1-arg call yet.

- [ ] **Step 3: Remove the dead code**

Replace the full contents of `src/main/java/com/spark/agent/kafka/KafkaProducerService.java` with:

```java
package com.spark.agent.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public CompletableFuture<SendResult<Object, Object>> sendRaw(String topic, String key, String jsonPayload) {
        return kafkaTemplate.send(topic, key, jsonPayload);
    }
}
```

- [ ] **Step 4: Run to confirm the test passes**

Run: `./gradlew test --tests "com.spark.agent.kafka.KafkaProducerServiceTest"`
Expected: PASS.

- [ ] **Step 5: Fix the architecture diagram in `CLAUDE.md`**

In `CLAUDE.md`, replace the fenced architecture block (currently lines 58-79):
```
MQTT (EMQX :1883)
  └─► MqttSubscriber          HiveMQ async client; reconnects automatically;
        │                     re-subscribes via addConnectedListener on each connect
        ▼
      TelemetryService         @Transactional; one call per MQTT message
        ├─► DeviceHeartbeatService.heartbeat()    Redis SETNX+EX; DB write only on
        │     └─► DeviceRepository.markOnline()   offline→online transition
        ├─► DeviceDataRepository.saveAll()        batch insert, snowflake IDs
        ├─► KafkaProducerService.sendTelemetry()  one message per property row
        └─► AlertService.evaluate()               per property row
              ├─► AlertRuleRepository.findActiveRules()
              ├─► AlertRecordRepository.countRecentUnhandled()  debounce
              ├─► AlertRecordRepository.save()
              └─► KafkaProducerService.sendAlert()

Redis key expiry (keyspace notification: __keyevent@0__:expired)
  └─► DeviceHeartbeatService.onMessage()   filters device:online:* keys
        └─► DeviceRepository.markOffline() DB write only on online→offline transition

ApiController  GET endpoints → DeviceDataRepository / AlertRecordRepository
```
with:
```
MQTT (EMQX :1883)
  └─► MqttSubscriber          HiveMQ async client; reconnects automatically;
        │                     re-subscribes via addConnectedListener on each connect
        ▼
      TelemetryService         @Transactional; one call per MQTT message
        ├─► DeviceHeartbeatService.heartbeat()    Redis SETNX+EX; DB write only on
        │     └─► DeviceRepository.markOnline()   offline→online transition
        ├─► DeviceDataRepository.saveAll()        batch insert, snowflake IDs
        ├─► OutboxMessageFactory.build() + OutboxMessageRepository.saveAll()  one outbox row per property row
        └─► AlertService.evaluate()               per property row
              ├─► AlertRuleRepository.findActiveRules()
              ├─► AlertRecordRepository.countRecentUnhandled()  debounce (pg_advisory_xact_lock-guarded)
              ├─► AlertRecordRepository.save()
              └─► OutboxMessageFactory.build() + OutboxMessageRepository.save()

OutboxRelayService (@Scheduled, polls aiot_outbox for unpublished rows)
  └─► KafkaProducerService.sendRaw()   publishes to iot.device.data / iot.alert.triggered, marks published_at
  └─► purge()  (@Scheduled, daily)     deletes published rows older than app.outbox-purge-retention-days

Redis key expiry (keyspace notification: __keyevent@0__:expired)
  └─► DeviceHeartbeatService.onMessage()   filters device:online:* keys
        └─► DeviceRepository.markOffline() DB write only on online→offline transition

ApiController  GET endpoints → DeviceDataRepository / AlertRecordRepository
```

- [ ] **Step 6: Fix the stale "Kafka send inside transaction" note**

In `CLAUDE.md`, replace (currently line 91):
```
**Kafka send inside transaction** — `KafkaProducerService` sends within the `@Transactional` scope of `TelemetryService`. If the DB transaction rolls back, the Kafka message is already sent (at-most-once). Acceptable for phase 1; upgrade to transactional outbox in phase 2 if needed.
```
with:
```
**Transactional outbox** — `TelemetryService` and `AlertService` write `OutboxMessage` rows in the same `@Transactional` scope as their business writes, instead of calling Kafka directly. `OutboxRelayService.relay()` polls unpublished rows on a schedule (`app.outbox-relay-interval-ms`) and publishes them via `KafkaProducerService.sendRaw()`, marking `published_at` on success. If the DB transaction rolls back, no outbox row exists, so nothing is published — this is now at-least-once, not at-most-once. `OutboxRelayService.purge()` deletes published rows older than `app.outbox-purge-retention-days` on a daily schedule.
```

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/kafka/KafkaProducerService.java src/test/java/com/spark/agent/kafka/KafkaProducerServiceTest.java CLAUDE.md
git commit -m "chore(kafka): remove dead direct-send methods, fix stale outbox docs"
```

---

## Final verification

- [ ] Run the full suite: `./gradlew build`
- [ ] Expected: BUILD SUCCESSFUL, all tests green including the two new `@SpringBootTest` integration tests (`AlertServiceConcurrencyTest` is new in Task 3; `DeviceDataRepositoryTest` already existed) — both require the local dev Postgres from `docker compose` to be running.
