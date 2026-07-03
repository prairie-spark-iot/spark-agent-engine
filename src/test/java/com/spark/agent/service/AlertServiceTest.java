package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.AlertOperator;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.AlertRule;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.AlertRuleRepository;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertServiceTest {

    @Mock
    private AlertRuleRepository alertRuleRepository;
    @Mock
    private AlertRecordRepository alertRecordRepository;
    @Mock
    private OutboxMessageRepository outboxMessageRepository;
    @Mock
    private OutboxMessageFactory outboxMessageFactory;
    @Mock
    private SnowflakeIdGenerator idGenerator;

    private AppProperties appProperties;
    private AlertService alertService;

    private DeviceData sampleData;
    private AlertRule sampleRule;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties();
        appProperties.setAlertDebounceMinutes(5);
        alertService = new AlertService(alertRuleRepository, alertRecordRepository,
                outboxMessageRepository, outboxMessageFactory, idGenerator, appProperties);

        sampleData = new DeviceData();
        sampleData.setDeviceId(1L);
        sampleData.setDeviceKey("DK_TEST_001");
        sampleData.setIdentifier("temperature");
        sampleData.setValueNum(new BigDecimal("150.5"));
        sampleData.setValue("150.5");
        sampleData.setReportTime(LocalDateTime.now());

        sampleRule = new AlertRule();
        sampleRule.setId(100L);
        sampleRule.setName("High Temperature");
        sampleRule.setOperator(AlertOperator.GT);
        sampleRule.setThreshold("100");
        sampleRule.setLevel((short) 2);
    }

    @Test
    void evaluate_nullValueNum_doesNothing() {
        sampleData.setValueNum(null);
        alertService.evaluate(sampleData);
        verifyNoInteractions(alertRuleRepository);
    }

    @Test
    void evaluate_noMatchingRules_noAlert() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of());
        alertService.evaluate(sampleData);
        verify(alertRuleRepository).findActiveRules(1L, "temperature");
        verifyNoInteractions(alertRecordRepository);
    }

    @Test
    void evaluate_matchingRule_createsAlertAndOutboxRow() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(0L);
        when(idGenerator.nextId()).thenReturn(999L);
        OutboxMessage outboxMessage = new OutboxMessage();
        when(outboxMessageFactory.build(eq("alert_record"), eq("999"), eq("alert.triggered"), any(AlertRecord.class)))
                .thenReturn(outboxMessage);

        alertService.evaluate(sampleData);

        verify(alertRecordRepository).save(any(AlertRecord.class));
        verify(outboxMessageFactory).build(eq("alert_record"), eq("999"), eq("alert.triggered"), any(AlertRecord.class));
        verify(outboxMessageRepository).save(same(outboxMessage));
    }

    @Test
    void evaluate_debounced_skipsAlert() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(1L);

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
        verify(outboxMessageRepository, never()).save(any());
    }

    @Test
    void evaluate_valueBelowThreshold_noAlert() {
        sampleData.setValueNum(new BigDecimal("50"));
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    // ---- matches() operator tests via evaluate ----

    @Test
    void evaluate_gtOperator_triggersWhenAbove() {
        sampleData.setValueNum(new BigDecimal("101"));
        testOperatorTriggers(AlertOperator.GT, "100");
    }

    @Test
    void evaluate_ltOperator_triggersWhenBelow() {
        sampleData.setValueNum(new BigDecimal("50"));
        testOperatorTriggers(AlertOperator.LT, "100");
    }

    @Test
    void evaluate_gteOperator_triggersWhenEqual() {
        sampleData.setValueNum(new BigDecimal("100"));
        testOperatorTriggers(AlertOperator.GTE, "100");
    }

    @Test
    void evaluate_lteOperator_triggersWhenEqual() {
        sampleData.setValueNum(new BigDecimal("100"));
        testOperatorTriggers(AlertOperator.LTE, "100");
    }

    @Test
    void evaluate_eqOperator_triggersOnExactMatch() {
        sampleData.setValueNum(new BigDecimal("99.9"));
        testOperatorTriggers(AlertOperator.EQ, "99.9");
    }

    @Test
    void evaluate_eqOperator_doesNotTriggerOnNearMatch() {
        sampleData.setValueNum(new BigDecimal("99.9001"));
        sampleRule.setOperator(AlertOperator.EQ);
        sampleRule.setThreshold("99.9");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void evaluate_neOperator_triggersOnDifferent() {
        sampleData.setValueNum(new BigDecimal("50"));
        testOperatorTriggers(AlertOperator.NE, "100");
    }

    @Test
    void evaluate_invalidThreshold_doesNotTrigger() {
        sampleRule.setThreshold("not-a-number");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void evaluate_unknownOperator_doesNotTrigger() {
        sampleRule.setOperator(AlertOperator.UNKNOWN);
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    private void testOperatorTriggers(AlertOperator operator, String threshold) {
        sampleRule.setOperator(operator);
        sampleRule.setThreshold(threshold);
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(0L);
        when(idGenerator.nextId()).thenReturn(999L);
        OutboxMessage outboxMessage = new OutboxMessage();
        when(outboxMessageFactory.build(eq("alert_record"), eq("999"), eq("alert.triggered"), any(AlertRecord.class)))
                .thenReturn(outboxMessage);

        alertService.evaluate(sampleData);

        verify(alertRecordRepository).save(any(AlertRecord.class));
        verify(outboxMessageRepository).save(same(outboxMessage));
    }
}
