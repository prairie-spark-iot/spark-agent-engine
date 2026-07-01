package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.repository.VectorStoreRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DiagnosisAgentServiceTest {

    @Mock
    private DeviceRepository deviceRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private DeviceDataRepository deviceDataRepository;
    @Mock
    private AlertRecordRepository alertRecordRepository;
    @Mock
    private RagSearchService ragSearchService;
    @Mock
    private ChatClient.Builder chatClientBuilder;
    @Mock
    private ChatClient chatClient;
    @Mock(answer = Answers.RETURNS_SELF)
    private ChatClient.ChatClientRequestSpec requestSpec;
    @Mock
    private ChatClient.CallResponseSpec callResponseSpec;
    @Mock
    private DiagnosisPromptBuilder promptBuilder;
    private AppProperties appProperties;

    private DiagnosisAgentService service;

    private AlertRecord sampleAlert;
    private Device sampleDevice;
    private Product sampleProduct;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties();
        service = new DiagnosisAgentService(deviceRepository, productRepository,
                deviceDataRepository, alertRecordRepository, ragSearchService,
                chatClientBuilder, appProperties, promptBuilder);

        when(chatClientBuilder.build()).thenReturn(chatClient);
        service.init();

        sampleAlert = new AlertRecord();
        sampleAlert.setId(100L);
        sampleAlert.setDeviceKey("DK_TEST");
        sampleAlert.setIdentifier("temperature");
        sampleAlert.setTriggerValue("150.5");
        sampleAlert.setLevel((short) 2);
        sampleAlert.setAlertContent("High temperature alert");
        sampleAlert.setTriggerTime(LocalDateTime.now());
        sampleAlert.setDiagnosisStatus((short) 0);

        sampleDevice = new Device();
        sampleDevice.setDeviceKey("DK_TEST");
        sampleDevice.setDeviceName("Test Device");
        sampleDevice.setProductId(1L);

        sampleProduct = new Product();
        sampleProduct.setId(1L);
        sampleProduct.setProductKey("PK_TEST");
    }

    @Test
    void diagnose_alertNotFound_throws() {
        when(alertRecordRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(jakarta.persistence.EntityNotFoundException.class,
                () -> service.diagnose(999L));
    }

    @Test
    void diagnose_inferenceFailure_returnsErrorResult() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST", (short) 0))
                .thenReturn(Optional.of(sampleDevice));
        when(productRepository.findById(1L)).thenReturn(Optional.of(sampleProduct));
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());
        when(alertRecordRepository.findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                "DK_TEST", (short) 0)).thenReturn(List.of());
        when(ragSearchService.search(anyString(), any(), anyInt())).thenReturn(List.of());
        when(promptBuilder.formatManuals(anyList())).thenReturn("none found");
        when(promptBuilder.formatTelemetry(anyList(), anyString())).thenReturn("no data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(false)))
                .thenReturn("test prompt");
        when(promptBuilder.getSystemPrompt()).thenReturn("test system prompt");

        // Simulate LLM failure
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenThrow(new RuntimeException("Ollama unavailable"));

        DiagnosisResult result = service.diagnose(100L);

        assertEquals(0, result.confidence());
        assertTrue(result.diagnosisDetail().contains("Inference failed"));
    }

    @Test
    void diagnose_deviceNotFound_resolvesNullModel() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST", (short) 0))
                .thenReturn(Optional.empty());
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());
        when(alertRecordRepository.findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                "DK_TEST", (short) 0)).thenReturn(List.of());
        when(ragSearchService.search(anyString(), isNull(), anyInt())).thenReturn(List.of());
        when(promptBuilder.formatManuals(anyList())).thenReturn("none found");
        when(promptBuilder.formatTelemetry(anyList(), anyString())).thenReturn("no data");
        when(promptBuilder.buildUserPrompt(any(), isNull(), anyString(), anyString(), anyString(), eq(false)))
                .thenReturn("test prompt");
        when(promptBuilder.getSystemPrompt()).thenReturn("test system prompt");

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenThrow(new RuntimeException("Ollama unavailable"));

        DiagnosisResult result = service.diagnose(100L);

        assertNotNull(result);
        verify(ragSearchService).search(anyString(), isNull(), anyInt());
    }

    @Test
    void diagnose_highConfidence_setsDiagnosisStatus2() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST", (short) 0))
                .thenReturn(Optional.of(sampleDevice));
        when(productRepository.findById(1L)).thenReturn(Optional.of(sampleProduct));
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());
        when(alertRecordRepository.findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                "DK_TEST", (short) 0)).thenReturn(List.of());
        // Provide non-empty RAG results so reflection is NOT triggered
        VectorStoreRepository.SearchResult manual = new VectorStoreRepository.SearchResult(
                1L, "manual", "content", (short) 1, "PK_TEST", "source", 0.5);
        when(ragSearchService.search(anyString(), any(), anyInt())).thenReturn(List.of(manual));
        when(promptBuilder.formatManuals(anyList())).thenReturn("manual excerpt");
        when(promptBuilder.formatTelemetry(anyList(), anyString())).thenReturn("no data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(false)))
                .thenReturn("test prompt");
        when(promptBuilder.getSystemPrompt()).thenReturn("test system prompt");

        DiagnosisResult llmResult = new DiagnosisResult(
                "root cause", "fix suggestion", 95, "detailed analysis");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(llmResult);

        DiagnosisResult result = service.diagnose(100L);

        assertEquals("root cause", result.rootCause());
        assertEquals("fix suggestion", result.suggestion());
        assertEquals(95, result.confidence());

        verify(alertRecordRepository).save(argThat(record -> {
            assertEquals("root cause", record.getRootCause());
            assertEquals("fix suggestion", record.getSuggestion());
            assertEquals(BigDecimal.valueOf(95), record.getConfidence());
            assertEquals((short) 2, record.getDiagnosisStatus());
            assertNotNull(record.getDiagnosisTime());
            return true;
        }));
    }

    @Test
    void diagnose_lowConfidence_triggersReflection() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST", (short) 0))
                .thenReturn(Optional.of(sampleDevice));
        when(productRepository.findById(1L)).thenReturn(Optional.of(sampleProduct));
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());
        when(alertRecordRepository.findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                "DK_TEST", (short) 0)).thenReturn(List.of());
        when(ragSearchService.search(anyString(), any(), anyInt())).thenReturn(List.of());
        when(promptBuilder.formatManuals(anyList())).thenReturn("none found");
        when(promptBuilder.formatTelemetry(anyList(), anyString())).thenReturn("no data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(false)))
                .thenReturn("initial prompt");
        when(promptBuilder.getSystemPrompt()).thenReturn("test system prompt");

        DiagnosisResult lowConfResult = new DiagnosisResult("guess", "maybe", 30, "low confidence");
        DiagnosisResult reflectionResult = new DiagnosisResult("real cause", "real fix", 85, "reflected analysis");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class))
                .thenReturn(lowConfResult)
                .thenReturn(reflectionResult);

        when(deviceDataRepository
                .findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                        eq("DK_TEST"), eq((short) 0), any(LocalDateTime.class), any()))
                .thenReturn(List.of());
        when(promptBuilder.formatTelemetry(anyList(), contains("last")))
                .thenReturn("widened data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(true)))
                .thenReturn("reflection prompt");

        DiagnosisResult result = service.diagnose(100L);

        assertEquals("real cause", result.rootCause());
        assertEquals(85, result.confidence());
        assertTrue(result.diagnosisDetail().contains("Initial Analysis"));
        assertTrue(result.diagnosisDetail().contains("Reflection Retry"));
    }

    @Test
    void diagnose_reflectionStillLowConfidence_writesHumanReviewStatus() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST", (short) 0))
                .thenReturn(Optional.of(sampleDevice));
        when(productRepository.findById(1L)).thenReturn(Optional.of(sampleProduct));
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());
        when(alertRecordRepository.findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                "DK_TEST", (short) 0)).thenReturn(List.of());
        when(ragSearchService.search(anyString(), any(), anyInt())).thenReturn(List.of());
        when(promptBuilder.formatManuals(anyList())).thenReturn("none found");
        when(promptBuilder.formatTelemetry(anyList(), anyString())).thenReturn("no data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(false)))
                .thenReturn("initial");
        when(promptBuilder.getSystemPrompt()).thenReturn("system prompt");

        DiagnosisResult lowConfResult = new DiagnosisResult("guess", "maybe", 30, "low");
        DiagnosisResult stillLowResult = new DiagnosisResult("guess2", "maybe2", 35, "still low");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class))
                .thenReturn(lowConfResult)
                .thenReturn(stillLowResult);

        when(deviceDataRepository
                .findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                        eq("DK_TEST"), eq((short) 0), any(LocalDateTime.class), any()))
                .thenReturn(List.of());
        when(promptBuilder.formatTelemetry(anyList(), contains("last")))
                .thenReturn("widened data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(true)))
                .thenReturn("reflection");

        service.diagnose(100L);

        verify(alertRecordRepository).save(argThat(record ->
                record.getDiagnosisStatus() == 1));
    }

    @Test
    void diagnose_writeBack_clearsDeletedFlag() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));
        when(deviceRepository.findByDeviceKeyAndDeleted("DK_TEST", (short) 0))
                .thenReturn(Optional.of(sampleDevice));
        when(productRepository.findById(1L)).thenReturn(Optional.of(sampleProduct));
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());
        when(alertRecordRepository.findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                "DK_TEST", (short) 0)).thenReturn(List.of());
        VectorStoreRepository.SearchResult manual = new VectorStoreRepository.SearchResult(
                1L, "manual", "content", (short) 1, "PK_TEST", "source", 0.5);
        when(ragSearchService.search(anyString(), any(), anyInt())).thenReturn(List.of(manual));
        when(promptBuilder.formatManuals(anyList())).thenReturn("manual excerpt");
        when(promptBuilder.formatTelemetry(anyList(), anyString())).thenReturn("no data");
        when(promptBuilder.buildUserPrompt(any(), any(), anyString(), anyString(), anyString(), eq(false)))
                .thenReturn("prompt");
        when(promptBuilder.getSystemPrompt()).thenReturn("system");

        DiagnosisResult goodResult = new DiagnosisResult("cause", "fix", 90, "detail");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(goodResult);

        service.diagnose(100L);

        verify(alertRecordRepository).save(argThat(record ->
                record.getDeleted() == 0));
    }
}
