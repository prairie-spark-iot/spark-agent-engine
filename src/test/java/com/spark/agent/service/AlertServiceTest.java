package com.spark.agent.service;

import com.spark.agent.common.ConflictException;
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
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

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
    @Mock
    private EntityManager entityManager;
    @Mock
    private Query nativeQuery;

    private AppProperties appProperties;
    private AlertService alertService;

    private DeviceData sampleData;
    private AlertRule sampleRule;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties();
        appProperties.setAlertDebounceMinutes(5);

        lenient().when(entityManager.createNativeQuery(anyString())).thenReturn(nativeQuery);
        lenient().when(nativeQuery.setParameter(anyInt(), any())).thenReturn(nativeQuery);
        lenient().when(nativeQuery.getSingleResult()).thenReturn(null);

        alertService = new AlertService(alertRuleRepository, alertRecordRepository,
                outboxMessageRepository, outboxMessageFactory, idGenerator, appProperties, entityManager);

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
    void evaluate_secondCallWithinTtl_reusesCachedRules() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(1L); // debounced both times — keeps this test focused on caching, not insert side-effects

        alertService.evaluate(sampleData);
        alertService.evaluate(sampleData);

        verify(alertRuleRepository, times(1)).findActiveRules(1L, "temperature");
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
    void evaluate_matchingRule_populatesRuleOperatorAndThreshold() {
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(0L);
        when(idGenerator.nextId()).thenReturn(999L);
        when(outboxMessageFactory.build(eq("alert_record"), eq("999"), eq("alert.triggered"), any(AlertRecord.class)))
                .thenReturn(new OutboxMessage());

        alertService.evaluate(sampleData);

        verify(alertRecordRepository).save(argThat(record -> {
            assertEquals("gt", record.getRuleOperator());
            assertEquals("100", record.getRuleThreshold());
            return true;
        }));
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

    // ---- requestDiagnosis() ----

    @Test
    void requestDiagnosis_alertNotFound_throwsEntityNotFound() {
        when(alertRecordRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(EntityNotFoundException.class, () -> alertService.requestDiagnosis(999L));
    }

    @Test
    void requestDiagnosis_pendingAlert_setsRequestedAtAndPublishesOutboxRow() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 0);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));
        OutboxMessage outboxMessage = new OutboxMessage();
        when(outboxMessageFactory.build(eq("alert_record"), eq("42"), eq("alert.triggered"), same(record)))
                .thenReturn(outboxMessage);

        AlertRecord result = alertService.requestDiagnosis(42L);

        assertNotNull(result.getDiagnosisRequestedAt());
        verify(alertRecordRepository).save(record);
        verify(outboxMessageRepository).save(same(outboxMessage));
    }

    @Test
    void requestDiagnosis_alreadyRequested_throwsConflict() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 0);
        record.setDiagnosisRequestedAt(LocalDateTime.now());
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        assertThrows(ConflictException.class, () -> alertService.requestDiagnosis(42L));
        verify(alertRecordRepository, never()).save(any());
        verify(outboxMessageRepository, never()).save(any());
    }

    @Test
    void requestDiagnosis_alreadyDiagnosed_throwsConflict() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 2);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        assertThrows(ConflictException.class, () -> alertService.requestDiagnosis(42L));
    }

    @Test
    void requestDiagnosis_humanReviewRequired_throwsConflict() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 1);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        assertThrows(ConflictException.class, () -> alertService.requestDiagnosis(42L));
    }

    // ---- approveAlert() ----

    @Test
    void approveAlert_alertNotFound_throwsEntityNotFound() {
        when(alertRecordRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(EntityNotFoundException.class, () -> alertService.approveAlert(999L));
    }

    @Test
    void approveAlert_stillPending_throwsConflict() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 0);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        assertThrows(ConflictException.class, () -> alertService.approveAlert(42L));
        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void approveAlert_diagnosed_setsHandleStatusAndApprovedAt() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 2);
        record.setHandleStatus((short) 0);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        AlertRecord result = alertService.approveAlert(42L);

        assertEquals((short) 1, result.getHandleStatus());
        assertNotNull(result.getApprovedAt());
        verify(alertRecordRepository).save(record);
    }

    @Test
    void approveAlert_humanReviewRequired_alsoApprovable() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 1);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        AlertRecord result = alertService.approveAlert(42L);

        assertEquals((short) 1, result.getHandleStatus());
    }

    @Test
    void approveAlert_calledTwice_isIdempotentAndKeepsFirstApprovedAtTimestamp() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 2);
        when(alertRecordRepository.findById(42L)).thenReturn(Optional.of(record));

        AlertRecord first = alertService.approveAlert(42L);
        LocalDateTime firstApprovedAt = first.getApprovedAt();

        AlertRecord second = alertService.approveAlert(42L);

        assertEquals((short) 1, second.getHandleStatus());
        assertEquals(firstApprovedAt, second.getApprovedAt());
    }
}
