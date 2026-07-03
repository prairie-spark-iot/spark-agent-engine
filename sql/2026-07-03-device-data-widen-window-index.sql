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
