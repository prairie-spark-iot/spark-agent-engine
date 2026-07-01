-- Speeds up DeviceDataRepository.findLatestByDeviceKey and
-- findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc
-- (used by the queryDeviceStatus and queryDeviceHistory MCP tools, and by the
-- pre-existing GET /api/device/{deviceKey}/latest REST endpoint).
-- Without it, both queries fall back to a full table scan; at ~80k rows in
-- aiot_device_data this measured ~68s. With it, ~200ms.
--
-- Schema is managed manually in this project (spring.jpa.hibernate.ddl-auto: none,
-- no migration tooling) — this file is the tracked record of that manual change.
-- Already applied to the running dev instance; run manually against any other
-- environment (staging/prod) before relying on these code paths at scale.
CREATE INDEX CONCURRENTLY idx_device_data_key_identifier_time
  ON aiot_device_data (device_key, identifier, report_time DESC)
  WHERE deleted = 0;
