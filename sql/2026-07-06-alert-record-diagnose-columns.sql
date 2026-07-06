-- Supports two Phase 1/2 integration-contract additions (see
-- spark-agent-docs/phase-1-2-api-data-contracts.md):
--
-- 1. rule_operator / rule_threshold: denormalized copy of the AlertRule that matched
--    at the moment this alert fired (AlertService.buildRecord()). An alert record must
--    keep showing the threshold that was in effect when it triggered, not whatever the
--    rule says today if it's edited/deleted later by spark-iot-agent's admin UI — hence
--    a copy on the record rather than a read-time join to aiot_alert_rule.
--
-- 2. diagnosis_requested_at: marks the moment an operator (or the BFF) asks for an
--    on-demand diagnosis via POST /api/alerts/{id}/diagnose, distinct from
--    diagnosis_time (when the LLM actually finished). The gap between the two is how
--    the API/BFF derives a transient "Diagnosing" state without a stored status enum
--    value for it.
--
-- Schema is managed manually in this project (spring.jpa.hibernate.ddl-auto: none,
-- no migration tooling) — this file is the tracked record of that manual change.
-- Apply manually to any environment; DDL execution against aiot_* is owned by
-- spark-iot-agent per this workspace's ownership rule, this repo only tracks the change.
ALTER TABLE aiot_alert_record ADD COLUMN IF NOT EXISTS rule_operator VARCHAR(8);
ALTER TABLE aiot_alert_record ADD COLUMN IF NOT EXISTS rule_threshold VARCHAR(64);
ALTER TABLE aiot_alert_record ADD COLUMN IF NOT EXISTS diagnosis_requested_at TIMESTAMP;
