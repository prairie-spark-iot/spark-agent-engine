# Phase 2 — Transactional Outbox Pattern (Backlog)

> Replaces the current at-most-once Kafka delivery with exactly-once semantics
> via the transactional outbox pattern.

## Motivation

`TelemetryService.process()` is `@Transactional`. Inside that transaction, it calls
`KafkaProducerService.sendTelemetry(row)` and `KafkaProducerService.sendAlert(record)`,
which do a fire-and-forget `kafkaTemplate.send()` — the `CompletableFuture` is
never joined. If the DB transaction rolls back after the Kafka send succeeds, the
Kafka message is already out (ghost message). If the Kafka send fails, the DB
transaction still commits (silent data loss).

This is a **known design limitation** documented in AGENTS.md as acceptable for
Phase 1. Phase 2 eliminates it.

## Affected Components

| Component | Current | Target |
|---|---|---|
| `TelemetryService.process()` | Fire-and-forget Kafka inside `@Transactional` | Write to outbox table instead |
| `KafkaProducerService` | `kafkaTemplate.send()` with unjoined future | Retired (or kept for non-critical topics) |
| `AlertService.evaluate()` | `kafkaProducerService.sendAlert()` inside same `@Transactional` | Write to outbox instead |
| New: `OutboxRepository` | — | JPA repository for `outbox` table |
| New: `OutboxPublisher` | — | Scheduled/polled sender that queries outbox, sends Kafka, deletes on ack |
| New: `Outbox` entity | — | JPA entity: `id`, `topic`, `key`, `payload` (JSON), `created_at`, `sent_at` |

## Design Sketch

### Option A (Recommended): Reliable Outbox with Polling Publisher

1. Add an `aiot_outbox` table (managed by the same `ddl-auto: none` external schema):
   ```sql
   CREATE TABLE aiot_outbox (
       id BIGINT PRIMARY KEY,
       topic VARCHAR(128) NOT NULL,
       message_key VARCHAR(255),
       payload JSONB NOT NULL,
       created_at TIMESTAMP NOT NULL DEFAULT now(),
       sent_at TIMESTAMP
   );
   CREATE INDEX idx_outbox_unsent ON aiot_outbox (created_at) WHERE sent_at IS NULL;
   ```

2. `TelemetryService.process()` and `AlertService.evaluate()` write `Outbox` rows
   inside the existing `@Transactional` instead of calling Kafka directly.

3. A `@Scheduled` `OutboxPublisher` polls `aiot_outbox WHERE sent_at IS NULL
   ORDER BY created_at LIMIT 100`, sends each row to Kafka, and deletes or marks
   `sent_at = now()` on ack.

4. On publish failure, the row stays unsent and is retried on the next poll cycle.
   Add a `maxRetries` / `dead_letter` field for poison messages.

### Option B: CDC-based (Debezium + Kafka Connect)

Instead of polling, stream outbox rows via Postgres WAL (Debezium connector) into
Kafka. Avoids polling latency and DB load. Higher operational complexity — requires
Debezium deployment and Kafka Connect.

## Migration Path

1. Create the `aiot_outbox` table (external schema management).
2. Add `Outbox` entity + `OutboxRepository`.
3. Modify `TelemetryService.process()` and `AlertService.evaluate()`: replace
   `kafkaProducerService.send*()` with `outboxRepository.save()`.
4. Implement `OutboxPublisher` with `@Scheduled(fixedDelay = 500)`.
5. Configure dead-letter handling for persistently failing outbox rows.
6. Delete or deprecate `KafkaProducerService` (or keep for non-critical topics
   that can tolerate at-most-once).
7. Remove `KafkaTemplate` injection from `TelemetryService` and `AlertService`.

## Open Questions

- **Ordering guarantee**: Outbox rows for the same device key should go to the
  same Kafka partition. Use `message_key = deviceKey` and Kafka's default
  partitioner. The polling publisher must send rows in creation order per key.
- **Idempotent consumers**: Downstream consumers must handle duplicate delivery
  (the publisher may send and crash before marking `sent_at`). All consumers
  should use idempotent processing (e.g. upsert on message ID).
- **Polling interval vs. latency**: `@Scheduled(fixedDelay = 500)` adds up to
  500ms of latency. For lower latency, use `@Scheduled(fixedDelay = 100)` or
  switch to Option B (CDC).

## Acceptance Criteria

- [ ] All telemetry property rows and alert records are persisted in `aiot_outbox`
      within the same DB transaction as the primary data.
- [ ] Outbox rows are delivered to Kafka within 1 second of commit (p95).
- [ ] No ghost messages or silent data loss when Kafka is unavailable during
      the DB transaction.
- [ ] Dead-letter mechanism prevents infinite retry loops on poison messages.
- [ ] Build passes, existing test suite passes.

## References

- AGENTS.md: "Kafka at-most-once — known design limitation, acceptable for phase 1"
- Microservices Pattern by Chris Richardson: Transactional Outbox
- Debezium Outbox Event Router: https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
