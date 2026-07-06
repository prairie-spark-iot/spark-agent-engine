package com.spark.agent.service;

import com.spark.agent.common.ConflictException;
import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.AlertRule;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.AlertRuleRepository;
import com.spark.agent.repository.OutboxMessageRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.emptyList;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertService {

    private record CacheEntry<V>(V value, long expiresAt) {
        boolean isValid() { return System.currentTimeMillis() < expiresAt; }
    }
    private final Map<String, CacheEntry<List<AlertRule>>> rulesCache = new ConcurrentHashMap<>();
    private static final long RULES_CACHE_TTL_MS = 5_000;
    private static final long RULES_CACHE_JITTER_MS = 1_000;
    private static final int RULES_CACHE_MAX_SIZE = 10_000;

    private List<AlertRule> getActiveRules(Long deviceId, String identifier) {
        String key = deviceId + ":" + identifier;
        CacheEntry<List<AlertRule>> entry = rulesCache.get(key);
        if (entry != null && entry.isValid()) {
            return entry.value();
        }
        List<AlertRule> rules = alertRuleRepository.findActiveRules(deviceId, identifier);
        long ttl = RULES_CACHE_TTL_MS + ThreadLocalRandom.current().nextLong(RULES_CACHE_JITTER_MS);
        rulesCache.put(key, new CacheEntry<>(rules, System.currentTimeMillis() + ttl));
        if (rulesCache.size() > RULES_CACHE_MAX_SIZE) {
            evictStaleCacheEntries();
        }
        return rules;
    }

    /**
     * Remove expired entries from the rules cache to prevent unbounded memory growth.
     * Called when the cache exceeds the configured max size.
     */
    private void evictStaleCacheEntries() {
        int before = rulesCache.size();
        rulesCache.entrySet().removeIf(e -> !e.getValue().isValid());
        log.debug("[Alert] Rules cache eviction: {} → {} entries ({} stale removed)",
                before, rulesCache.size(), before - rulesCache.size());
    }

    private final AlertRuleRepository alertRuleRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final OutboxMessageRepository outboxMessageRepository;
    private final OutboxMessageFactory outboxMessageFactory;
    private final SnowflakeIdGenerator idGenerator;
    private final AppProperties appProperties;
    private final EntityManager entityManager;

    @Transactional
    public void evaluate(DeviceData data) {
        if (data.getValueNum() == null) return;

        List<AlertRule> rules = getActiveRules(data.getDeviceId(), data.getIdentifier());
        double value = data.getValueNum().doubleValue();

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
    }

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

    private boolean matches(AlertRule rule, double value) {
        try {
            double threshold = Double.parseDouble(rule.getThreshold());
            return rule.getOperator().matches(value, threshold);
        } catch (NumberFormatException e) {
            log.warn("[Alert] Unparseable threshold '{}' for rule {}", rule.getThreshold(), rule.getId());
            return false;
        }
    }

    private boolean isDebounced(Long deviceId, Long ruleId) {
        LocalDateTime since = LocalDateTime.now().minusMinutes(appProperties.getAlertDebounceMinutes());
        return alertRecordRepository.countRecentUnhandled(deviceId, ruleId, since) > 0;
    }

    private static final short DIAGNOSIS_STATUS_PENDING = 0;

    /**
     * Handles POST /api/alerts/{id}/diagnose. Deliberately does NOT call the LLM directly —
     * Ollama's inference slot is already serialized to concurrency=1 via the
     * iot.alert.triggered consumer (AlertTriggeredConsumer, groupId=diagnosis-agent), so an
     * on-demand trigger must go through the same queue rather than around it. This method only
     * flips diagnosis_requested_at and drops a message on the existing outbox/Kafka pipeline;
     * the consumer picks it up whenever the single Ollama slot is free. See
     * spark-agent-docs/phase-1-2-api-data-contracts.md §3.
     */
    @Transactional
    public AlertRecord requestDiagnosis(Long alertId) {
        AlertRecord record = alertRecordRepository.findById(alertId)
                .orElseThrow(() -> new EntityNotFoundException("AlertRecord " + alertId + " not found"));

        if (record.getDiagnosisRequestedAt() != null || record.getDiagnosisStatus() != DIAGNOSIS_STATUS_PENDING) {
            throw new ConflictException("Alert " + alertId + " is already Diagnosing or Diagnosed");
        }

        record.setDiagnosisRequestedAt(LocalDateTime.now());
        alertRecordRepository.save(record);
        outboxMessageRepository.save(
                outboxMessageFactory.build("alert_record", String.valueOf(record.getId()), "alert.triggered", record));
        return record;
    }

    /**
     * Handles POST /api/alerts/{id}/approve. Reuses handle_status (0=unhandled, 1=handled)
     * as the "approved" signal rather than adding a separate boolean column — the frontend's
     * diagnosis.approved is derived from handleStatus === 1 (alertAdapter.ts). Idempotent: a
     * second call just re-confirms handleStatus=1 without overwriting the first approved_at,
     * since re-approving shouldn't erase when it was actually first approved.
     */
    @Transactional
    public AlertRecord approveAlert(Long alertId) {
        AlertRecord record = alertRecordRepository.findById(alertId)
                .orElseThrow(() -> new EntityNotFoundException("AlertRecord " + alertId + " not found"));

        if (record.getDiagnosisStatus() == DIAGNOSIS_STATUS_PENDING) {
            throw new ConflictException("Alert " + alertId + " has not been diagnosed yet");
        }

        record.setHandleStatus((short) 1);
        if (record.getApprovedAt() == null) {
            record.setApprovedAt(LocalDateTime.now());
        }
        alertRecordRepository.save(record);
        return record;
    }

    private AlertRecord buildRecord(DeviceData data, AlertRule rule, double value) {
        AlertRecord r = new AlertRecord();
        r.setId(idGenerator.nextId());
        r.setRuleId(rule.getId());
        r.setDeviceId(data.getDeviceId());
        r.setDeviceKey(data.getDeviceKey());
        r.setIdentifier(data.getIdentifier());
        r.setTriggerValue(String.valueOf(value));
        r.setLevel(rule.getLevel());
        r.setRuleOperator(rule.getOperator().code());
        r.setRuleThreshold(rule.getThreshold());
        r.setAlertContent(String.format("设备 %s 属性 %s 当前值 %s 触发规则「%s」(阈值: %s %s)",
                data.getDeviceKey(), data.getIdentifier(), data.getValue(),
                rule.getName(), rule.getOperator().code(), rule.getThreshold()));
        r.setTriggerTime(data.getReportTime());
        r.setDiagnosisStatus((short) 0);
        r.setHandleStatus((short) 0);
        return r;
    }
}
