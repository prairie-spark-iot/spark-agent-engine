package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.entity.OutboxMessage;
import com.spark.agent.kafka.KafkaProducerService;
import com.spark.agent.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock
    private OutboxMessageRepository outboxMessageRepository;
    @Mock
    private KafkaProducerService kafkaProducerService;

    private ObjectMapper objectMapper;
    private AppProperties appProperties;
    private OutboxRelayService relayService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        appProperties = new AppProperties();
        appProperties.setOutboxRelayBatchSize(100);
        relayService = new OutboxRelayService(outboxMessageRepository, kafkaProducerService, objectMapper, appProperties);
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

    @SuppressWarnings("unchecked")
    private CompletableFuture<SendResult<Object, Object>> completedSend() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }

    @Test
    void relay_emptyBatch_doesNothing() {
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of());

        relayService.relay();

        verifyNoInteractions(kafkaProducerService);
        verify(outboxMessageRepository, never()).markPublished(any(), any());
    }

    @Test
    void relay_successfulSend_marksPublished() {
        OutboxMessage msg = outboxMessage(1L, "device.data", "DK_TEST_001");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));
        when(kafkaProducerService.sendRaw("iot.device.data", "DK_TEST_001", msg.getPayload()))
                .thenReturn(completedSend());

        relayService.relay();

        verify(kafkaProducerService).sendRaw("iot.device.data", "DK_TEST_001", msg.getPayload());
        verify(outboxMessageRepository).markPublished(eq(1L), any(LocalDateTime.class));
    }

    @Test
    void relay_alertEventType_resolvesAlertTopic() {
        OutboxMessage msg = outboxMessage(2L, "alert.triggered", "DK_TEST_002");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));
        when(kafkaProducerService.sendRaw("iot.alert.triggered", "DK_TEST_002", msg.getPayload()))
                .thenReturn(completedSend());

        relayService.relay();

        verify(kafkaProducerService).sendRaw("iot.alert.triggered", "DK_TEST_002", msg.getPayload());
        verify(outboxMessageRepository).markPublished(eq(2L), any(LocalDateTime.class));
    }

    @Test
    void relay_sendFailure_leavesRowUnpublishedAndContinuesBatch() {
        OutboxMessage failing = outboxMessage(3L, "device.data", "DK_TEST_003");
        OutboxMessage succeeding = outboxMessage(4L, "device.data", "DK_TEST_004");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(failing, succeeding));

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException("kafka unreachable", new RuntimeException()));
        when(kafkaProducerService.sendRaw("iot.device.data", "DK_TEST_003", failing.getPayload()))
                .thenReturn(failedFuture);
        when(kafkaProducerService.sendRaw("iot.device.data", "DK_TEST_004", succeeding.getPayload()))
                .thenReturn(completedSend());

        relayService.relay();

        verify(outboxMessageRepository, never()).markPublished(eq(3L), any());
        verify(outboxMessageRepository).markPublished(eq(4L), any(LocalDateTime.class));
    }

    @Test
    void relay_unknownEventType_skipsRowWithoutThrowing() {
        OutboxMessage msg = outboxMessage(5L, "unknown.type", "DK_TEST_005");
        when(outboxMessageRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(msg));

        assertDoesNotThrow(() -> relayService.relay());

        verifyNoInteractions(kafkaProducerService);
        verify(outboxMessageRepository, never()).markPublished(any(), any());
    }

    @Test
    void purge_deletesPublishedRowsOlderThanRetention() {
        appProperties.setOutboxPurgeRetentionDays(7);
        when(outboxMessageRepository.deletePublishedBefore(any(LocalDateTime.class))).thenReturn(3);

        relayService.purge();

        verify(outboxMessageRepository).deletePublishedBefore(any(LocalDateTime.class));
    }
}
