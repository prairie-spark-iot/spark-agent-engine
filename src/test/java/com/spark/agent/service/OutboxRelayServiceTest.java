package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock
    private OutboxMessageRepository outboxMessageRepository;
    @Mock
    private OutboxPublisher outboxPublisher;

    private AppProperties appProperties;
    private OutboxRelayService relayService;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties();
        appProperties.setOutboxRelayBatchSize(100);
        relayService = new OutboxRelayService(outboxMessageRepository, outboxPublisher, new tools.jackson.databind.ObjectMapper(), appProperties);
    }

    private OutboxMessage outboxMessage(long id, String eventType, String deviceKey) {
        OutboxMessage msg = new OutboxMessage();
        msg.setId(id);
        msg.setAggregateType(eventType.equals("device.data") ? "device_data" : "alert_record");
        msg.setAggregateId(String.valueOf(id));
        msg.setEventType(eventType);
        msg.setPayload("{\"deviceKey\":\"" + deviceKey + "\"}");
        msg.setCreatedAt(LocalDateTime.now());
        return msg;
    }

    @Test
    void relay_emptyBatch_doesNothing() {
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of());

        relayService.relay();

        verifyNoInteractions(outboxPublisher);
    }

    @Test
    void relay_successfulSend_delegatesToPublisher() {
        OutboxMessage msg = outboxMessage(1L, "device.data", "DK_TEST_001");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));

        relayService.relay();

        verify(outboxPublisher).publish(msg, "iot.device.data", "DK_TEST_001");
    }

    @Test
    void relay_alertEventType_resolvesAlertTopic() {
        OutboxMessage msg = outboxMessage(2L, "alert.triggered", "DK_TEST_002");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));

        relayService.relay();

        verify(outboxPublisher).publish(msg, "iot.alert.triggered", "DK_TEST_002");
    }

    @Test
    void relay_publishFailure_continuesBatch() {
        OutboxMessage failing = outboxMessage(3L, "device.data", "DK_TEST_003");
        OutboxMessage succeeding = outboxMessage(4L, "device.data", "DK_TEST_004");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(failing, succeeding));
        doThrow(new RuntimeException("kafka unreachable"))
                .when(outboxPublisher).publish(failing, "iot.device.data", "DK_TEST_003");

        assertDoesNotThrow(() -> relayService.relay());

        verify(outboxPublisher).publish(failing, "iot.device.data", "DK_TEST_003");
        verify(outboxPublisher).publish(succeeding, "iot.device.data", "DK_TEST_004");
    }

    @Test
    void relay_unknownEventType_skipsRowWithoutThrowing() {
        OutboxMessage msg = outboxMessage(5L, "unknown.type", "DK_TEST_005");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));

        assertDoesNotThrow(() -> relayService.relay());

        verifyNoInteractions(outboxPublisher);
    }

    @Test
    void purge_deletesPublishedRowsOlderThanRetention() {
        appProperties.setOutboxPurgeRetentionDays(7);
        when(outboxMessageRepository.deletePublishedBefore(any(LocalDateTime.class))).thenReturn(3);

        relayService.purge();

        verify(outboxMessageRepository).deletePublishedBefore(any(LocalDateTime.class));
    }
}
