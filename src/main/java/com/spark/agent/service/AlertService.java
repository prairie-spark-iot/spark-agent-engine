package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.AlertRule;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.AlertRuleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertService {

    private final AlertRuleRepository alertRuleRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final KafkaProducerService kafkaProducerService;
    private final SnowflakeIdGenerator idGenerator;
    private final AppProperties appProperties;

    public void evaluate(DeviceData data) {
        if (data.getValueNum() == null) return;

        List<AlertRule> rules = alertRuleRepository.findActiveRules(data.getDeviceId(), data.getIdentifier());
        double value = data.getValueNum().doubleValue();

        for (AlertRule rule : rules) {
            if (!matches(rule, value)) continue;
            if (isDebounced(data.getDeviceId(), rule.getId())) continue;

            AlertRecord record = buildRecord(data, rule, value);
            alertRecordRepository.save(record);
            kafkaProducerService.sendAlert(record);
            log.info("[Alert] Rule '{}' triggered for {} {}: {}", rule.getName(), data.getDeviceKey(), data.getIdentifier(), value);
        }
    }

    private boolean matches(AlertRule rule, double value) {
        try {
            double threshold = Double.parseDouble(rule.getThreshold());
            double epsilon = 1e-10;
            return switch (rule.getOperator()) {
                case "gt"  -> value > threshold;
                case "lt"  -> value < threshold;
                case "gte" -> value >= threshold;
                case "lte" -> value <= threshold;
                case "eq"  -> Math.abs(value - threshold) < epsilon;
                case "ne"  -> Math.abs(value - threshold) >= epsilon;
                default -> {
                    log.warn("[Alert] Unknown operator: {}", rule.getOperator());
                    yield false;
                }
            };
        } catch (NumberFormatException e) {
            log.warn("[Alert] Unparseable threshold '{}' for rule {}", rule.getThreshold(), rule.getId());
            return false;
        }
    }

    private boolean isDebounced(Long deviceId, Long ruleId) {
        LocalDateTime since = LocalDateTime.now().minusMinutes(appProperties.getAlertDebounceMinutes());
        return alertRecordRepository.countRecentUnhandled(deviceId, ruleId, since) > 0;
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
        r.setAlertContent(String.format("设备 %s 属性 %s 当前值 %s 触发规则「%s」(阈值: %s %s)",
                data.getDeviceKey(), data.getIdentifier(), data.getValue(),
                rule.getName(), rule.getOperator(), rule.getThreshold()));
        r.setTriggerTime(data.getReportTime());
        r.setDiagnosisStatus((short) 0);
        r.setHandleStatus((short) 0);
        return r;
    }
}
