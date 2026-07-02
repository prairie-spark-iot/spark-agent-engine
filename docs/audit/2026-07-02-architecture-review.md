# Backend Architecture Review: `spark-agent-engine`

**Date:** 2026-07-02
**Reviewer:** Backend Architect (AI)

---

## Executive Summary

The `spark-agent-engine` represents a **Phase 1 MVP** that successfully delivers on its core mandate — ingesting IoT telemetry over MQTT, persisting to PostgreSQL, publishing to Kafka, evaluating alert rules, and running AI diagnosis via Ollama/Spring AI with MCP tool integration. The architecture uses modern Java 25 stack (Boot 4.1, Hivemq MQTT 5, Jackson 3, Hibernate 7) with good defaults across the board.

**Overall Grade: B+ for Phase 1; Phase 2-readiness assessed at C+**

The system has solid fundamentals but carries structural risks that will compound at scale: an at-most-once Kafka guarantee inside `@Transactional` boundaries, minimal API hardening (no auth, no rate limiting, no input validation beyond the trivial), and a test coverage base that covers critical services but has blind spots in the hot data path (`TelemetryService`, `MqttSubscriber`). The AI/Diagnosis layer is functional but tightly coupled and has a fragile timeout/resilience story. The infrastructure choices (Snowflake IDs, manual DDL, composite indexes, virtual threads, Redis event-driven heartbeat) are strong.

---

## 1. Architecture Pattern Assessment

### 1.1 Modular Monolith → Correct Choice for Phase 1

The single-module monolith with clear service decomposition boundaries (TelemetryService, AlertService, HeartbeatService, DiagnosisAgentService) is **the right choice** for this stage. There is no justification for microservices given:
- Single team ownership
- Tight coupling in the telemetry-alert-diagnosis data flow
- No independent scaling requirements yet

However, the services are not truly bounded — `TelemetryService.process()` directly invokes `AlertService.evaluate()` and `KafkaProducerService.sendTelemetry()` synchronously in a loop. This creates a **tight procedural coupling** where failure in alert evaluation or Kafka send cannot be cleanly isolated.

**Recommendation (Phase 2)**: Introduce a lightweight in-process event bus or a `TelemetryPipeline` abstraction that allows alert evaluation and Kafka publishing to be extracted into independently observable stages, keeping the `@Transactional` write path as a separate concern.

### 1.2 Communication Patterns

| Flow | Pattern | Assessment |
|---|---|---|
| MQTT → Process | Async callback via VirtualThread pool | **Good** — avoids blocking the Netty event loop |
| Process → DB | Synchronous `@Transactional` | **Good** for correctness |
| Process → Kafka | Fire-and-forget (CompletableFuture never joined) | **Known limitation** — acknowledged; Phase 2 requires outbox |
| Process → Alert | Synchronous, same thread | **Reasonable** for MVP |
| Alert → Diagnosis | Kafka consumer (`iot.alert.triggered`) | **Good** — async decoupling |
| API | REST (GET only, no mutation) | **Adequate** for Phase 1; needs auth + rate limiting |

---

## 2. Data Consistency Model — Critical Gap

### 2.1 The At-Most-Once Kafka Problem

This is the **#1 architectural risk** in the system:

```java
// TelemetryService.process() — the critical path
@Transactional
public void process(DeviceTelemetryMessage msg) {
    // ...
    deviceDataRepository.saveAll(rows);   // DB insert
    deviceDataRepository.flush();

    for (DeviceData row : rows) {
        kafkaProducerService.sendTelemetry(row);  // Fire-and-forget Kafka
        alertService.evaluate(row);               // May send more Kafka messages
    }
}
```

**Failure scenario**: DB transaction rolls back after Kafka messages are already sent (partition leader acknowledgment happens before the commit). The Kafka messages arrive at consumers (`iot.device.data`, `iot.alert.triggered`) but the corresponding DB rows are lost. Downstream systems (analytics, dashboards, AI diagnosis) see phantom data.

**Impact severity**: HIGH — This is a data consistency breach between the system of record (PostgreSQL) and the event stream (Kafka). In production, this means:
- Dashboard/reporting systems will show counts that don't match the database
- AI diagnosis may trigger on alerts that were rolled back
- Data lakes ingesting `iot.device.data` will have stale phantom entries

**The solution** (already planned for Phase 2 — transactional outbox):

```java
// Phase 2 pattern
@Transactional
public void process(DeviceTelemetryMessage msg) {
    // 1. Write DeviceData rows + OutboxMessage rows in SAME transaction
    deviceDataRepository.saveAll(rows);
    outboxRepository.saveAll(buildOutboxMessages(rows));
    // 2. After commit, a relay process publishes from outbox table to Kafka
    //    using at-least-once with deduplication
}
```

This is a well-understood pattern (Change Data Capture via outbox). Given the fire-and-forget Kafka is already acknowledged as a known limitation, I'm not downgrading the review — but this **must be the first Phase 2 deliverable**.

### 2.2 Alert Service: Same Kafka Problem, Plus Debounce Race

```java
// AlertService.evaluate()
synchronized (debounceLock(data.getDeviceId(), rule.getId())) {
    if (isDebounced(...)) continue;
    AlertRecord record = buildRecord(data, rule, value);
    alertRecordRepository.save(record);     // DB insert
    kafkaProducerService.sendAlert(record); // Fire-and-forget Kafka
}
```

The alert generates an `AlertRecord` and publishes it to `iot.alert.triggered`. The downstream consumer (`AlertTriggeredConsumer`) reads the Kafka message, deserializes it, and passes only the `alertId` to `DiagnosisAgentService.diagnose()`. This is a **critical design detail**: the consumer is tolerant of HTTP timeout (deserialization of unknown fields), but the `diagnose()` method does a fresh DB lookup (`alertRecordRepository.findById(alertId)`). If the producer's DB transaction rolled back, the consumer gets an `EntityNotFoundException`.

This is the **correct consumer design** for an at-most-once producer — the consumer re-validates against the system of record. However, this doubles the DB reads and adds latency. With the outbox pattern in Phase 2, the consumer can trust the message body.

### 2.3 Redis Heartbeat — Correct Use of SET NX EX

```java
// DeviceHeartbeatService.heartbeat()
Boolean wasAbsent = redisTemplate.opsForValue().setIfAbsent(key, "1", ttl);
if (Boolean.TRUE.equals(wasAbsent)) {
    deviceRepository.markOnline(deviceId, LocalDateTime.now()); // DB write only on transition
} else {
    redisTemplate.expire(key, ttl); // TTL refresh only, no DB write
}
```

The offline→online detection is correct: `SET NX EX` is atomic. The online→offline detection via Redis keyspace notifications (`__keyevent@0__:expired`) has a **correct race guard**:

```java
// DeviceHeartbeatService.onMessage()
Boolean stillAbsent = redisTemplate.hasKey(redisKey);
if (Boolean.TRUE.equals(stillAbsent)) return; // device reconnected before we processed the expiry
```

**Risk**: If Redis goes down, all devices immediately appear offline in the DB and no new heartbeat keys can be written. Consider a **fail-open strategy** — if Redis is unavailable, treat devices as online (or last-known state) rather than marking everything offline. This is a Phase 2 consideration.

---

## 3. Database & Schema Review

### 3.1 Manual DDL Strategy — Correct for IoT

`ddl-auto: none` is the **right choice** for this system. IoT schema changes require careful migration planning, index management, and zero-downtime deployments that Hibernate's auto-DDL cannot deliver. The external management system (`spark-iot-agent`) owning the DDL is a sound separation of concerns.

### 3.2 Snowflake ID Generator — Good with Caveats

```java
@Component
public class SnowflakeIdGenerator {
    private final long machineId; // configurable via app.snowflake.machine-id
    // 41-bit timestamp (from 2021-01-01) | 10-bit machine | 12-bit sequence
    // Clock rollback → throws IllegalStateException
}
```

**Strengths**:
- Configurable machine ID for multi-instance deployment
- Clock rollback detection with explicit exception (no silent corruption)
- No DB dependency for ID generation (fast, offline-safe)

**Risks**:
- Single machine ID limits to 1 instance per `machineId` value. For horizontal scaling, each instance needs a unique `machineId` (up to 1024 with 10 bits).
- The `synchronized` method on `nextId()` creates contention under high throughput — for IoT telemetry (1000s of msg/s with multiple properties), this could become a bottleneck. Consider `LongAdder` or a per-thread sequence.

### 3.3 Composite Index — Critical for Query Performance

The `idx_device_data_key_identifier_time` composite index on `(device_key, identifier, report_time DESC) WHERE deleted = 0` is documented as reducing full-table scans from 68s to fast indexed access. This is **well-designed** for the query patterns:

```java
// findLatestByDeviceKey uses correlated subquery
// Both history queries use device_key + identifier + report_time
```

**Gap**: The `findLatestByDeviceKey` query uses a correlated subquery (`MAX(d2.reportTime)` per identifier group). At scale (100k+ rows per device), the correlated subquery may still scan all rows for each identifier. Consider a **materialized "latest snapshot" view** or **window function approach** for Phase 2.

### 3.4 HikariCP Configuration — Appropriate

```yaml
hikari:
  maximum-pool-size: 10
  auto-commit: false
  connection-timeout: 30000
```

Pool size of 10 is adequate for the current workload (MQTT ingestion is serialized per virtual thread, Kafka consumers are limited to 6). `auto-commit: false` ensures `@Transactional` owns commit boundaries.

**Gap**: No `leak-detection-threshold` set. For Phase 2, add `leak-detection-threshold: 10000` to catch connection leaks early.

---

## 4. AI/Diagnosis Layer Review

### 4.1 DiagnosisAgentService — Functional but Fragile

```java
@Transactional
public DiagnosisResult diagnose(Long alertId) {
    AlertRecord alert = alertRecordRepository.findById(alertId)...;
    String userPrompt = /* inline string template */;
    DiagnosisResult result = runInference(userPrompt);  // LLM call (up to 60s)
    writeBack(alert, result);                            // DB writeback
    return result;
}
```

**Strengths**:
- `CompletableFuture.orTimeout(60, TimeUnit.SECONDS)` prevents indefinite hangs
- LLM inference failure returns a graceful error result (confidence=0)
- Tool callbacks (`deviceToolCallbacks`) are properly injected via Spring AI
- Confidence threshold gates auto-diagnosis vs. human review

**Structural issues**:

1. **Prompt construction is inlined** in `DiagnosisAgentService` despite `DiagnosisPromptBuilder` existing per AGENTS.md. The Wave 2 refactor may not be reflected in code. If it is still inline, this violates the documented architecture.

2. **LLM call inside @Transactional**: The `runInference()` method can take up to 60 seconds (LLM inference + tool calls). This holds a HikariCP connection for the entire duration — a **connection leak risk**. For Phase 2, restructure as:
   ```java
   // Read alert OUTSIDE transaction (read-only)
   AlertRecord alert = alertRecordRepository.findById(alertId)...;
   // Run LLM inference (no DB connection held)
   DiagnosisResult result = runInference(userPrompt);
   // Writeback in SEPARATE short-lived transaction
   writeBackInNewTransaction(alert, result);
   ```

3. **No retry circuit breaker**: If Ollama is consistently failing (e.g., model not loaded), every alert diagnosis attempt will fail for 60 seconds. The 6 concurrent Kafka consumers will all be blocked. Consider a circuit breaker (`Resilience4j`) for Phase 2.

4. **No idempotency guard**: If the same alert is re-delivered (Kafka at-least-once with DLQ retry), `diagnose()` runs full inference again and overwrites the previous diagnosis. Add a check before inference: if `diagnosisStatus != 0`, skip.

### 4.2 MCP Tool Layer — Well-Designed

```java
@Service
public class DeviceMcpToolService {
    @Tool(description = "List all devices...")     public List<DeviceSummary> listDevices();
    @Tool(description = "Query a device's...")     public DeviceStatusResult queryDeviceStatus(...);
    @Tool(description = "Query historical...")     public List<DeviceData> queryDeviceHistory(...);
    @Tool(description = "Query recent alert...")   public List<AlertRecord> queryDeviceAlerts(...);
    @Tool(description = "Search the device...")    public List<SearchResult> queryDeviceManual(...);
}
```

- Tool annotations use Spring AI's `@Tool`/`@ToolParam` — clean declarative approach
- All query methods are capped at 500 rows — prevents unbounded LLM context
- `queryDeviceManual` correctly routes through `RagSearchService` for pgvector search
- The `listDevices()` tool gives the LLM a discovery capability before querying specific devices

**Gap**: The tools are **synchronous** and will hold DB connections during LLM inference. With Ollama running locally, tool calls are fast — but if the LLM chains multiple tool calls, each blocks the Kafka consumer thread. Consider caching `listDevices()` results with a short TTL.

### 4.3 RAG / Vector Store — Functional Baseline

- `VectorStoreRepository.saveEmbedding()` uses `INSERT ... ON CONFLICT DO UPDATE` (upsert) — correct
- `VectorStoreRepository.search()` uses pgvector `<->` operator with parameterized queries — safe from SQL injection
- `KnowledgeIngestionService.ingest()` correctly runs embeddings **outside** the `@Transactional` boundary to avoid holding DB connections during Ollama HTTP calls
- `KnowledgeIngestionService.saveChunks()` uses **row-by-loop** `save()` + `saveEmbedding()` — this is N round-trips. For Phase 2, batch insert chunks and embeddings.

---

## 5. API Layer Review

### 5.1 REST Endpoints — Minimal but Functional

```
GET /api/device/{deviceKey}/latest
GET /api/device/{deviceKey}/history?identifier=<id>&limit=<n>
GET /api/alert/recent?limit=<n>
POST /api/knowledge/import       body: List<KnowledgeImportItem>
POST /api/rag/ingest             body: IngestRequest (@Valid)
POST /api/rag/search             body: SearchRequest
```

**Critical Gaps (all documented in AGENTS.md)**:

| Gap | Severity | Phase 2 Priority |
|---|---|---|
| No authentication | HIGH | P0 |
| No rate limiting | HIGH | P0 |
| No input validation (beyond `@Valid` on RagController) | MEDIUM | P1 |
| API returns JPA entities directly (exposing `deleted`, `tenantId` via `@JsonIgnore`, but still coupling response to persistence model) | MEDIUM | P1 |
| `POST /api/knowledge/import` accepts unbounded list bodies (capped to 50 in controller, but no body size limits) | LOW | P2 |
| No CORS configuration | LOW | P2 |
| No request ID / tracing headers | MEDIUM | P1 |

### 5.2 GlobalExceptionHandler — Adequate

The `@RestControllerAdvice` covers:
- `EntityNotFoundException` → 404
- `IllegalArgumentException` → 400
- `MethodArgumentTypeMismatchException` → 400
- `HttpMessageNotReadableException` → 400
- `MethodArgumentNotValidException` → 400 (with field-level error aggregation)
- `ConstraintViolationException` → 400
- `Exception` → 500 (catch-all)

**Good**: Proper error code differentiation. Field-level validation errors are aggregated.

**Gap**: The 500 handler exposes `ex.getMessage()` to the client — a potential information leak. For Phase 2, mask internal error details for production.

### 5.3 R Response Wrapper — Clean

```java
public class R<T> { int code; String msg; T data; }
R.ok(data)    → {code:0, msg:"success", data:...}
R.fail(msg)   → {code:500, msg:..., data:null}
R.fail(code, msg) → {code:<code>, msg:..., data:null}
```

Consistent shape. The `code:0` convention is a Chinese enterprise convention (Ruoyi ecosystem). No issues here.

---

## 6. Infrastructure & Observability

### 6.1 Kafka Configuration — Well-Tuned

```yaml
producer:
  acks: all
  retries: 3
  batch-size: 16384
  linger-ms: 5
  compression-type: lz4
  enable.idempotence: true
```

These are production-grade settings. `acks=all` + idempotence ensures no duplicate messages from producer retries. `lz4` compression is appropriate for JSON payloads.

**Consumer DLQ**: The `KafkaConsumerConfig` implements a `DeadLetterPublishingRecoverer` with 2 retries (2s backoff) before routing to `<topic>.DLT`. This is **excellent** — previously, failed diagnosis records were silently dropped.

### 6.2 Observability — Basic

Actuator exposes `health`, `info`, `metrics`. No distributed tracing (no Micrometer Tracing / Zipkin / OpenTelemetry configured). No structured logging correlation IDs.

**Phase 2** should add:
- `spring-boot-starter-actuator` already provides metrics; add `micrometer-registry-prometheus` for scraping
- Distributed tracing via Micrometer Tracing (Spring Boot 4 supports it out of the box)
- Correlation IDs propagated from MQTT → Service → Kafka → Consumer

### 6.3 Redis Observability — One Concern

The `RedisKeyExpirationConfig` uses `InitializingBean` to `CONFIG SET notify-keyspace-events Ex`. This is documented as potentially failing on cloud Redis instances with ACL restrictions. The fallback is a warning log — acceptable for Phase 1. Phase 2 should add a health indicator that verifies the config took effect.

---

## 7. Test Coverage Analysis

### 7.1 What's Tested (Good)

| Component | Tests | Quality |
|---|---|---|
| `AlertService` | 12 tests | **Excellent** — covers all operators, debounce, threshold parsing edge cases, null guard |
| `DiagnosisAgentService` | 6 tests | **Good** — covers not-found, inference failure, tool callback wiring, confidence gating, deleted flag |
| `ApiController` | 6 tests | **Good** — covers limit capping (0, 9999), 404 response, default vs custom limits |
| `DeviceMcpToolService` | 3 tests | **Adequate** — covers listDevices mapping, offline device, empty list |
| `GlobalExceptionHandler` | test file exists | Needs review |
| `SnowflakeIdGenerator` | test file exists | Needs review |

### 7.2 Critical Test Gaps (HIGH PRIORITY)

| Component | Risk | What to Test |
|---|---|---|
| **`TelemetryService.process()`** | No tests at all | The core data pipeline. Test: device lookup (found/not-found), property parsing, DB rollback scenarios, Kafka send interaction |
| **`MqttSubscriber`** | No tests | Message deserialization, error handling, virtual thread delegation |
| **`DeviceHeartbeatService`** | No tests | Redis SET NX EX semantics, keyspace notification processing, race condition guard |
| **`KafkaProducerService`** | No tests | Serialization, send failure handling |
| **`KnowledgeIngestionService.importBatch()`** | No transactional test | Row-by-row failure isolation, embedding failure within batch |
| **Integration tests** | None | The `SparkAgentEngineApplicationTests` is a single `contextLoads()`. No test that verifies Docker containers are up. |

### 7.3 Test Quality Observations

The existing tests use **Mockito without Spring context** — they are true unit tests. This is appropriate and fast. However, the absence of any integration test (Testcontainers or `@SpringBootTest` with Docker) means the actual infrastructure wiring (MQTT → transactional → Kafka → Redis) has **never been validated programmatically**.

---

## 8. Priority-Ordered Phase 2 Recommendations

### P0 — Must Fix (Data Integrity & Security)

1. **Transactional Outbox Pattern**
   - Replace fire-and-forget Kafka sends with an outbox table (`aiot_outbox`): `id`, `aggregate_type`, `aggregate_id`, `event_type`, `payload`, `created_at`, `published_at`
   - Implement relay process (scheduled task or Debezium) to publish after commit
   - Add deduplication on consumer side using `aggregate_id` + `event_type`
   - **This is the only path to exactly-once semantics**

2. **API Hardening**
   - Add authentication (API key or JWT, depending on consumer profile)
   - Add rate limiting (Bucket4j or Spring Cloud Gateway if fronted)
   - Add request body size limits
   - Add `@Validated` on `ApiController` with parameter constraints

3. **DiagnosisAgentService Transaction Restructure**
   - Split: read alert (read-only) → LLM inference (no DB connection) → writeback (short-lived transaction)
   - Add pre-inference idempotency guard (`if diagnosisStatus != 0, skip`)
   - Add circuit breaker for Ollama failures (Resilience4j `@CircuitBreaker`)

### P1 — High Priority (Scale & Reliability)

4. **TelemetryService Test Coverage**
   - Unit tests for `process()` with mocked dependencies
   - Test the DB rollback → Kafka scenario to document the exact at-most-once behavior (until outbox)

5. **Observability Upgrade**
   - Prometheus metrics export (`micrometer-registry-prometheus`)
   - Distributed tracing (Micrometer Tracing with OTLP export)
   - Correlation ID propagation: MQTT message → `X-Correlation-ID` header/log field → Kafka header
   - SLO definitions: MQTT-to-DB latency p95 < 200ms, diagnosis pipeline latency p95 < 45s (LLM-bound)

6. **API DTO Layer**
   - Introduce response DTOs (`DeviceLatestResponse`, `AlertRecordResponse`) that decouple from JPA entities
   - This prevents accidentally exposing internal fields and allows API evolution independent of schema

7. **RAG Performance Optimization**
   - Batch `saveChunks()` / `insertKnowledge()` with JDBC batch operations instead of row-by-loop
   - Add embedding model circuit breaker
   - Consider pgvector HNSW index for larger knowledge bases

### P2 — Medium Priority (Operational Excellence)

8. **Integration Test Suite**
   - Testcontainers-based tests: PostgreSQL + Kafka + Redis
   - MQTT message → DB write → Kafka message → consumer verification end-to-end
   - Heartbeat expiry → offline transition test

9. **SnowflakeIdGenerator Throughput**
   - Replace `synchronized` with lock-free approach for high-throughput ingestion (1000+ msg/s)
   - Add integration test for concurrent ID generation across threads

10. **DeviceData Query Optimization**
    - Evaluate materialized "latest snapshot" for the correlated subquery pattern
    - Add query plan monitoring for regression detection

11. **CORS / Security Headers**
    - Configure CORS explicitly (default is permissive)
    - Add security headers (Content-Security-Policy, X-Content-Type-Options)

12. **Redis Fail-Open Strategy**
    - If Redis is unavailable, device heartbeat should not transition devices to offline
    - Add Redis health gating to heartbeat logic

### P3 — Enhancement (Future)

13. **DiagnosisPromptBuilder Actualization**
    - Extract prompt construction from `DiagnosisAgentService` to `DiagnosisPromptBuilder` (as documented in AGENTS.md as "Wave 2 refactor")
    - This makes prompt logic unit-testable without mocking ChatClient

14. **Reflection Retry Implementation**
    - AGENTS.md mentions "reflection retry if confidence < 40% or no RAG results found, retries once with widened telemetry window (120 min)" — this pattern is not visible in the current `DiagnosisAgentService` code. Implement or remove the documentation discrepancy.

15. **AlertOperator Enum**
    - Replace `String` operator (`"gt"`, `"lt"`, etc.) with a proper Java enum for type safety

16. **Tenant Isolation**
    - `tenantId` defaults to `1L` everywhere. If multi-tenancy is planned, add tenant context propagation through all layers. Ensure all queries respect `tenantId` filter (current queries use hardcoded `deleted = 0` but no `tenantId` filter).

---

## Appendix: Quick Reference — Top 5 Action Items

| # | Action | Priority | Impact |
|---|---|---|---|
| 1 | Implement transactional outbox pattern for Kafka sends | P0 | Fixes at-most-once data consistency breach |
| 2 | Add auth + rate limiting to REST API | P0 | Unlocks production deployment |
| 3 | Restructure DiagnosisAgentService: split transaction, add idempotency guard | P0 | Prevents connection leaks, duplicate diagnoses |
| 4 | Add TelemetryService unit tests | P1 | Critical path currently has zero test coverage |
| 5 | Upgrade observability (Prometheus + tracing + correlation IDs) | P1 | Enables SLO monitoring and debugging at scale |