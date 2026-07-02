package com.spark.agent.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.ToolCallbackProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AssistantServiceTest {

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

    private AssistantService service;

    @BeforeEach
    void setUp() {
        service = new AssistantService(chatClientBuilder, deviceToolCallbacks);
        when(chatClientBuilder.build()).thenReturn(chatClient);
        service.init();
    }

    @Test
    void answer_returnsLlmContent() {
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.content()).thenReturn("2号注塑机最近1小时内触发了1次压力过高告警。");

        String result = service.answer("2号注塑机最近有什么异常");

        assertEquals("2号注塑机最近1小时内触发了1次压力过高告警。", result);
    }

    @Test
    void answer_usesToolCallbacks() {
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.content()).thenReturn("some answer");

        service.answer("some question");

        verify(requestSpec).tools(deviceToolCallbacks);
    }

    @Test
    void answer_llmFailure_throws() {
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.call()).thenThrow(new RuntimeException("Ollama unavailable"));

        assertThrows(RuntimeException.class, () -> service.answer("some question"));
    }
}
