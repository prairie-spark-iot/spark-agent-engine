# Device Latest-Data Query Window Function Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `DeviceDataRepository.findLatestByDeviceKey` to use a native SQL `ROW_NUMBER()` window function instead of a correlated subquery, cutting query latency (measured ~146ms → ~24ms at ~86k rows) while keeping the same index usage and the same method signature/behavior.

**Architecture:** Replace the JPQL `@Query` with a native SQL `@Query(nativeQuery = true)` using a derived table with `ROW_NUMBER() OVER (PARTITION BY identifier ORDER BY report_time DESC)`, filtering `rn = 1`. Verify correctness with a new `@SpringBootTest`-based repository integration test (the first of its kind in this codebase) against the real local Postgres, and verify the execution plan manually via `EXPLAIN ANALYZE` against the dev DB.

**Tech Stack:** Java 25, Spring Boot 4.1.0, Spring Data JPA, PostgreSQL, JUnit 5.

## Global Constraints

- No DB schema or index changes — `idx_device_data_key_identifier_time` on `(device_key, identifier, report_time DESC) WHERE deleted = 0` already exists and must be used by the new query as-is.
- Method signature stays `List<DeviceData> findLatestByDeviceKey(String deviceKey)` — no caller changes needed in `DeviceMcpToolService` or the REST API.
- The outer `SELECT` in the native query must list entity columns explicitly (not `SELECT *`) so the extra `rn` column from the inner query never reaches Hibernate's native-to-entity mapping.

---

### Task 1: Rewrite findLatestByDeviceKey and add a repository integration test

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/DeviceDataRepository.java:11-19`
- Create: `src/test/java/com/spark/agent/repository/DeviceDataRepositoryTest.java`

**Interfaces:**
- No interface changes — `findLatestByDeviceKey(String deviceKey): List<DeviceData>` keeps its existing signature.

- [ ] **Step 1: Write the failing integration test**

Create `src/test/java/com/spark/agent/repository/DeviceDataRepositoryTest.java`:

```java
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
```

- [ ] **Step 2: Run the test to verify it fails or passes against the old query**

Run: `./gradlew test --tests "com.spark.agent.repository.DeviceDataRepositoryTest"`
Expected: PASS — the existing correlated-subquery implementation is already correct, so this test validates behavior first before the rewrite (this is a characterization test for the rewrite, not a red/green TDD step, since the bug being fixed is performance, not correctness).

- [ ] **Step 3: Rewrite the query**

In `DeviceDataRepository.java`, replace:

```java
    @Query("""
            SELECT d FROM DeviceData d
            WHERE d.deviceKey = :deviceKey AND d.deleted = 0
              AND d.reportTime = (
                SELECT MAX(d2.reportTime) FROM DeviceData d2
                WHERE d2.deviceKey = :deviceKey AND d2.identifier = d.identifier AND d2.deleted = 0
              )
            ORDER BY d.identifier
            """)
    List<DeviceData> findLatestByDeviceKey(String deviceKey);
```

with:

```java
    @Query(value = """
            SELECT ranked.id, ranked.device_id, ranked.device_key, ranked.identifier, ranked.value,
                   ranked.value_num, ranked.quality, ranked.report_time, ranked.creator, ranked.create_time,
                   ranked.updater, ranked.update_time, ranked.deleted, ranked.tenant_id
            FROM (
                SELECT d.*,
                       ROW_NUMBER() OVER (PARTITION BY d.identifier ORDER BY d.report_time DESC) AS rn
                FROM aiot_device_data d
                WHERE d.device_key = :deviceKey AND d.deleted = 0
            ) ranked
            WHERE ranked.rn = 1
            ORDER BY ranked.identifier
            """, nativeQuery = true)
    List<DeviceData> findLatestByDeviceKey(String deviceKey);
```

- [ ] **Step 4: Run the test to verify it still passes**

Run: `./gradlew test --tests "com.spark.agent.repository.DeviceDataRepositoryTest"`
Expected: PASS (both tests) — confirms the rewritten native query returns identical results to the old JPQL version.

- [ ] **Step 5: Verify the execution plan against the dev database**

Run (adjust the device key to one with real data, e.g. `DK_INJ_001`):

```bash
docker exec spark-postgres psql -U root -d spark_ai -c "
EXPLAIN ANALYZE
SELECT ranked.id, ranked.device_id, ranked.device_key, ranked.identifier, ranked.value,
       ranked.value_num, ranked.quality, ranked.report_time, ranked.creator, ranked.create_time,
       ranked.updater, ranked.update_time, ranked.deleted, ranked.tenant_id
FROM (
    SELECT d.*,
           ROW_NUMBER() OVER (PARTITION BY d.identifier ORDER BY d.report_time DESC) AS rn
    FROM aiot_device_data d
    WHERE d.device_key = 'DK_INJ_001' AND d.deleted = 0
) ranked
WHERE ranked.rn = 1
ORDER BY ranked.identifier;
"
```

Expected: the plan shows `Bitmap Index Scan on idx_device_data_key_identifier_time` (or `Index Only Scan` on it) feeding a `WindowAgg`, with no correlated `SubPlan`, and execution time well under the ~146ms correlated-subquery baseline recorded in the spec.

- [ ] **Step 6: Run the full test suite**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL (no regressions in `DeviceMcpToolServiceTest`, `ApiControllerTest`, or any other test exercising `findLatestByDeviceKey` indirectly)

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/repository/DeviceDataRepository.java src/test/java/com/spark/agent/repository/DeviceDataRepositoryTest.java
git commit -m "perf(device-data): rewrite findLatestByDeviceKey with ROW_NUMBER window function"
```
