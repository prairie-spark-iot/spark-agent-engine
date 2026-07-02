# Transactional Outbox for Telemetry & Alert Events — Design

## Purpose

`TelemetryService.process()` and `AlertService.evaluate()` each write a DB row and then fire a Kafka message via `KafkaProducerService`, whose `send()` is async and fire-and-forget (`.thenAccept/.exceptionally`, no blocking, no propagation of failure to the caller). Because the Kafka call happens *inside* the same `@Transactional` method as the DB write:

- If the surrounding transaction rolls back after the Kafka call already fired, a message goes out for a DB row that never persisted.
- If the Kafka send silently fails (broker down, serialization error), the DB row persists but no event is ever published — a "phantom" gap with no record that anything was missed.

This design introduces the Transactional Outbox pattern: instead of calling Kafka directly, `TelemetryService` and `AlertService` write an outbox row in the *same* DB transaction as the business row. A separate scheduled relay polls unpublished outbox rows and publishes them to Kafka, marking them published only on confirmed send. DB write and event publication become atomically consistent (the outbox row and the business row commit or roll back together); actual Kafka delivery becomes at-least-once via retry-until-published.

Covers original tasks 001–004. Tasks 005–008 (API validation, rate limiting, API key auth, DiagnosisAgentService resilience) are separate, independent subsystems and are out of scope for this spec.

## Components

| File | Change |
|---|---|
| `entity/OutboxMessage.java` | New. Standalone entity (does not extend `BaseEntity`) |
| `repository/OutboxMessageRepository.java` | New. `JpaRepository` + batch query for unpublished rows |
| `service/OutboxMessageFactory.java` | New. Tiny `@Component` shared by `TelemetryService` and `AlertService` to build an `OutboxMessage` (snowflake id, timestamp, JSON serialization) — both need identical logic, so it's a shared class, not duplicated private methods |
| `sql/2026-07-02-outbox-table.sql` | New. `aiot_outbox` table + partial index, following the manual-migration convention already used by `sql/2026-07-01-device-data-index.sql` (this repo has no `postgres-init.sql`; `ddl-auto: none`, schema changes are tracked as standalone timestamped files and applied by hand) |
| `service/TelemetryService.java` | Remove per-row `kafkaProducerService.sendTelemetry(row)`; build and save one `OutboxMessage` per `DeviceData` row in the same transaction |
| `service/AlertService.java` | Remove `kafkaProducerService.sendAlert(record)`; build and save one `OutboxMessage` alongside the `AlertRecord`. Add `@Transactional` directly to `evaluate()` (see Note below) |
| `kafka/KafkaProducerService.java` | Keep `sendTelemetry`/`sendAlert` as-is (unused for now, kept for future direct-send use cases per explicit instruction). Add `sendRaw(String topic, String key, String jsonPayload)` — sends a pre-serialized JSON string with no re-serialization, used only by the relay |
| `service/OutboxRelayService.java` | New. `@Scheduled` poll → publish → mark-published loop |
| `config/AppProperties.java` | Add `outboxRelayIntervalMs` (default 2000), `outboxRelayBatchSize` (default 100) |
| `application.yaml` | Add `app.outbox-relay-interval-ms`, `app.outbox-relay-batch-size` |

**Note on `AlertService.evaluate()`:** it is currently not itself `@Transactional` — it only gets transactional semantics today because its sole caller, `TelemetryService.process()`, is `@Transactional` and JPA save is deferred to flush. Since `evaluate()` now must atomically save both `AlertRecord` and its `OutboxMessage`, and shouldn't silently depend on caller context for correctness, it gets its own `@Transactional`. Called from within `TelemetryService`'s existing transaction, this just joins it (default `REQUIRED` propagation) — no behavior change for the current caller, but `evaluate()` becomes correct if ever called standalone.

## `OutboxMessage` Entity

```java
@Getter
@Setter
@Entity
@Table(name = "aiot_outbox")
public class OutboxMessage {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;   // "device_data" | "alert_record"

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;     // stringified snowflake id of the source row

    @Column(name = "event_type", nullable = false)
    private String eventType;       // "device.data" | "alert.triggered" — doubles as topic-lookup key

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;         // JSON, identical shape to what KafkaProducerService.send() used to serialize

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;  // null = unpublished
}
```

Not extending `BaseEntity`: `deleted`/`tenantId`/`creator`/`updater` don't mean anything for an append-only relay log, and every existing entity's use of `BaseEntity` is for soft-deletable, tenant-scoped business records, which this isn't.

## `aiot_outbox` Table

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

The partial index keeps the relay's poll query cheap regardless of how large the table grows with published (already-relayed) rows — published rows aren't indexed at all. No retention/cleanup job for published rows is included in this design (see Out of Scope).

## `OutboxMessageRepository`

```java
public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    List<OutboxMessage> findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable pageable);

    @Modifying
    @Transactional
    @Query("UPDATE OutboxMessage o SET o.publishedAt = :publishedAt WHERE o.id = :id")
    void markPublished(Long id, LocalDateTime publishedAt);
}
```

## `TelemetryService` Changes

```java
@Transactional
public void process(DeviceTelemetryMessage msg) {
    // ... unchanged device lookup / heartbeat ...

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
```

`OutboxMessageFactory.build(aggregateType, aggregateId, eventType, entity)` does the shared work: snowflake id via `idGenerator.nextId()`, `createdAt = LocalDateTime.now()`, `payload = objectMapper.writeValueAsString(entity)` (same `ObjectMapper` bean `KafkaProducerService` already uses, so the JSON shape is byte-identical to today's Kafka messages).

## `AlertService` Changes

```java
@Transactional
public void evaluate(DeviceData data) {
    // ... unchanged rule matching / debounce ...

    synchronized (debounceLock(data.getDeviceId(), rule.getId())) {
        if (isDebounced(data.getDeviceId(), rule.getId())) continue;

        AlertRecord record = buildRecord(data, rule, value);
        alertRecordRepository.save(record);
        outboxMessageRepository.save(outboxMessageFactory.build("alert_record", String.valueOf(record.getId()), "alert.triggered", record));
        log.info("[Alert] Rule '{}' triggered for {} {}: {}", rule.getName(), data.getDeviceKey(), data.getIdentifier(), value);
    }
}
```

## `KafkaProducerService.sendRaw`

```java
public CompletableFuture<SendResult<Object, Object>> sendRaw(String topic, String key, String jsonPayload) {
    return kafkaTemplate.send(topic, key, jsonPayload);
}
```

Returns the future directly (rather than swallowing it like `send()` does) so `OutboxRelayService` can block on it to confirm delivery before marking a row published.

## `OutboxRelayService`

```java
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

`relay()` itself is deliberately *not* `@Transactional` — wrapping the whole batch (including the blocking Kafka sends) in one DB transaction would hold a pooled connection for the entire batch's send latency, and a failure partway through would roll back `publishedAt` marks already committed for earlier, successfully-sent records in the same transaction. Instead, `@Transactional` sits directly on `OutboxMessageRepository.markPublished(...)` in the repository interface — Spring Data JPA repository proxies honor `@Transactional` placed on the interface method itself, applying the transactional advice at the repository bean (not at `OutboxRelayService`), so there's no self-invocation problem to worry about. Each row's mark-published commits independently, immediately after its own Kafka send is confirmed.

A single record's send failure is caught, logged, and the loop continues — matching the spec's "single failure doesn't affect the batch" requirement. No retry cap, no dead-letter table: an unpublished row simply reappears in the next `relay()` poll indefinitely.

## Data Flow

```
MQTT message → TelemetryService.process()  [single @Transactional]
  ├─► deviceDataRepository.saveAll(rows)
  ├─► outboxMessageRepository.saveAll([OutboxMessage(device.data) × N])
  └─► for each row: alertService.evaluate(row)  [joins same transaction]
        └─► on rule match: alertRecordRepository.save(record)
            outboxMessageRepository.save(OutboxMessage(alert.triggered))
  ⇒ COMMIT — device data, outbox rows, and any alert records all persist together or none do

OutboxRelayService.relay()  [@Scheduled every 2s]
  ├─► outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(limit 100)  [read-only]
  └─► for each row:
        topic = EVENT_TYPE_TOPICS[row.eventType]
        key   = parse "deviceKey" from row.payload
        kafkaProducerService.sendRaw(topic, key, row.payload).get(5s)
          success → markPublished(row.id, now())  [short tx]
          failure → log, leave unpublished, retried next cycle
```

## Error Handling

- DB transaction rollback in `TelemetryService.process()` (e.g. constraint violation) → the outbox `saveAll` in the same transaction rolls back too → no Kafka message is ever built for that data. This is the core guarantee this design adds.
- Kafka broker unreachable when the relay runs → `sendRaw(...).get(5, TimeUnit.SECONDS)` throws (timeout or `ExecutionException`) → caught, logged at WARN, row stays unpublished, retried next cycle. No message loss, bounded lag once Kafka recovers.
- Unknown `eventType` (shouldn't happen given the two hardcoded call sites, but defensive) → logged at ERROR, row permanently skipped (would need a manual data fix — acceptable since this can only happen from a code bug, not runtime data).
- `OutboxRelayService` itself never throws out of `relay()` — every per-row failure is caught inside the loop, so one bad row can't stop the scheduled job from processing the rest of the batch or from firing again next cycle.

## Testing

- `TelemetryServiceTest` (existing, if present, else new): verify `outboxMessageRepository.saveAll(...)` is called with one `OutboxMessage` per `DeviceData` row, `eventType = "device.data"`, and that no direct `kafkaProducerService.sendTelemetry` call happens anymore.
- `AlertServiceTest`: verify a rule match produces both an `AlertRecord` save and an `OutboxMessage` save (`eventType = "alert.triggered"`), and that debounced/non-matching evaluations produce neither.
- `OutboxRelayServiceTest`: mock `OutboxMessageRepository` + `KafkaProducerService`; verify a batch with one successful and one failing send results in exactly one `markPublished` call; verify unknown `eventType` is skipped without throwing.
- Manual integration check: run the app against the real docker stack, publish an MQTT telemetry message, confirm an `aiot_outbox` row appears with `published_at IS NULL`, then within ~2s confirm `published_at` gets set and the message actually lands on `iot.device.data` (`kafka-console-consumer` or the existing `kafka-get-offsets.sh` check from CLAUDE.md). Kill the DB transaction mid-flight (e.g. temporarily break `deviceDataRepository.saveAll` to throw) and confirm no `aiot_outbox` row is created for that message.

## Out of Scope

- Dead-letter table or max-retry cap for permanently-failing rows — an eventType-mismatch bug aside, any Kafka-reachability failure is expected to be transient and self-heal via indefinite retry.
- Retention/cleanup of published `aiot_outbox` rows — the table grows unboundedly under this design. If this becomes an operational problem, a separate cleanup job (e.g. delete rows with `published_at` older than N days) is a follow-up task, not bundled here.
- Ordering guarantees across the whole outbox table — the relay processes oldest-unpublished-first within a batch, but does not guarantee strict global ordering across concurrent producers or across batches (acceptable: the original fire-and-forget Kafka sends had no ordering guarantee either, beyond Kafka's own per-partition-key ordering, which `sendRaw`'s partition key preserves).
- Migrating `KafkaProducerService.sendTelemetry`/`sendAlert` callers elsewhere in the codebase — grep shows `TelemetryService` and `AlertService` are the only callers, so removing their call sites effectively makes these methods currently unused; kept per explicit instruction for future direct-send use cases, not deleted.
