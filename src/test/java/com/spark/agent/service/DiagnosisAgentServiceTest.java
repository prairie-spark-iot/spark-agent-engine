package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.repository.AlertRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.ToolCallbackProvider;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DiagnosisAgentServiceTest {

    @Mock
    private AlertRecordRepository alertRecordRepository;
    @Mock
    private ChatClient.Builder chatClientBuilder;
    @Mock
    private ChatClient chatClient;
    @Mock(answer = Answers.RETURNS_SELF)
    private ChatClient.ChatClientRequestSpec requestSpec;
    @Mock
    private ChatClient.CallResponseSpec callResponseSpec;
    @Mock
    private ToolCallbackProvider deviceToolCallbacks;
    private AppProperties appProperties;

    private DiagnosisAgentService service;
    private DiagnosisPromptBuilder promptBuilder;

    private AlertRecord sampleAlert;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties();
        promptBuilder = new DiagnosisPromptBuilder();
        service = new DiagnosisAgentService(alertRecordRepository, chatClientBuilder,
                deviceToolCallbacks, appProperties, promptBuilder);

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

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenThrow(new RuntimeException("Ollama unavailable"));

        DiagnosisResult result = service.diagnose(100L);

        assertEquals(0, result.confidence());
        assertTrue(result.diagnosisDetail().contains("Inference failed"));
    }

    @Test
    void diagnose_usesToolCallbacks() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult llmResult = new DiagnosisResult("root cause", "fix suggestion", 95, "detailed analysis");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(llmResult);

        service.diagnose(100L);

        verify(requestSpec).tools(deviceToolCallbacks);
    }

    @Test
    void diagnose_highConfidence_setsDiagnosisStatus2() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

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
    void diagnose_lowConfidence_writesHumanReviewStatus() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult lowConfResult = new DiagnosisResult("guess", "maybe", 35, "low confidence");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(lowConfResult);

        service.diagnose(100L);

        verify(alertRecordRepository).save(argThat(record ->
                record.getDiagnosisStatus() == 1));
    }

    @Test
    void diagnose_highConfidence_doesNotRetry() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult llmResult = new DiagnosisResult("root cause", "fix suggestion", 95, "detailed analysis");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(llmResult);

        service.diagnose(100L);

        verify(chatClient, times(2)).prompt();
    }

    @Test
    void diagnose_lowConfidence_retriesOnceWithWiderWindow() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult lowConfResult = new DiagnosisResult("guess", "maybe", 35, "low confidence");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(lowConfResult);

        service.diagnose(100L);

        verify(chatClient, times(4)).prompt();
        verify(requestSpec).user(promptBuilder.retryUserPrompt(sampleAlert));
    }

    @Test
    void diagnose_retryProducesHigherConfidence_writesBackRetryResult() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult firstResult = new DiagnosisResult("guess", "maybe", 35, "low confidence");
        DiagnosisResult retryResult = new DiagnosisResult("confirmed cause", "clear fix", 70, "wider-window analysis");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(firstResult, retryResult);

        DiagnosisResult result = service.diagnose(100L);

        assertEquals(70, result.confidence());
        assertEquals("confirmed cause", result.rootCause());
        verify(alertRecordRepository).save(argThat(record ->
                record.getConfidence().compareTo(BigDecimal.valueOf(70)) == 0));
    }

    @Test
    void diagnose_retryDoesNotImprove_keepsFirstResult() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult firstResult = new DiagnosisResult("first cause", "first fix", 35, "first analysis");
        DiagnosisResult retryResult = new DiagnosisResult("retry cause", "retry fix", 20, "retry analysis");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(firstResult, retryResult);

        DiagnosisResult result = service.diagnose(100L);

        assertEquals(35, result.confidence());
        assertEquals("first cause", result.rootCause());
    }

    @Test
    void diagnose_writeBack_clearsDeletedFlag() {
        when(alertRecordRepository.findById(100L)).thenReturn(Optional.of(sampleAlert));

        DiagnosisResult goodResult = new DiagnosisResult("cause", "fix", 90, "detail");
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.entity(DiagnosisResult.class)).thenReturn(goodResult);

        service.diagnose(100L);

        verify(alertRecordRepository).save(argThat(record ->
                record.getDeleted() == 0));
    }
}
