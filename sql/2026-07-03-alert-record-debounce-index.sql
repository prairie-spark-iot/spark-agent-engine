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
