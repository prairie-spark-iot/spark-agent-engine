package com.spark.agent.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class AssistantService {

    private static final String SYSTEM_PROMPT = """
            You are an industrial IoT operations assistant. You have tools to look up factory
            equipment: listDevices (list all devices with key, name, product model, and online
            status — use this first if the question refers to a device by name or description
            rather than its exact deviceKey), queryDeviceStatus, queryDeviceHistory,
            queryDeviceAlerts, and queryDeviceManual. Use these tools as needed to answer the
            operator's question, then reply in natural language, in the same language as the
            question. Be concise and specific; cite concrete values and timestamps where relevant.
            """;

    private final ChatClient.Builder chatClientBuilder;
    private final ToolCallbackProvider deviceToolCallbacks;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    /**
     * Answer a free-form operator question. The LLM decides for itself which device
     * tools (if any) to call before producing a natural-language answer.
     *
     * @param question the operator's question, in any language
     * @return the LLM's natural-language answer
     */
    public String answer(String question) {
        log.debug("[Assistant] question={}", question);
        return CompletableFuture.supplyAsync(() ->
                chatClient.prompt()
                        .system(SYSTEM_PROMPT)
                        .tools(deviceToolCallbacks)
                        .user(question)
                        .call()
                        .content()
        ).orTimeout(60, TimeUnit.SECONDS).join();
    }
}
