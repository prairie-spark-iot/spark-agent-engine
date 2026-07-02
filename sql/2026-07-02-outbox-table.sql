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
