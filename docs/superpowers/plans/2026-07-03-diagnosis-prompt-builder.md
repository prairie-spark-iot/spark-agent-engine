# DiagnosisPromptBuilder Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the inline prompt-construction logic in `DiagnosisAgentService` into a standalone, dependency-free `DiagnosisPromptBuilder` component so prompt text can be unit tested without a `ChatClient`.

**Architecture:** Add a new Spring `@Component` `DiagnosisPromptBuilder` in `com.spark.agent.service` holding the two system-prompt constants and the per-alert user-prompt formatting, currently inlined in `DiagnosisAgentService`. `DiagnosisAgentService` gets it injected as a new final field (picked up by the existing `@RequiredArgsConstructor`) and delegates to it at the three call sites. No behavioral change.

**Tech Stack:** Java 25, Spring Boot 4.1.0, JUnit 5, Mockito.

## Global Constraints

- No behavior change to the diagnosis flow — `diagnose()` must produce byte-identical prompts to today.
- `DiagnosisPromptBuilder` must have zero dependencies (no `ChatClient`, no repositories) so it is testable with plain JUnit.
- Follow existing codebase pattern: Lombok `@RequiredArgsConstructor` for constructor injection.

---

### Task 1: Create DiagnosisPromptBuilder with unit tests

**Files:**
- Create: `src/main/java/com/spark/agent/service/DiagnosisPromptBuilder.java`
- Create: `src/test/java/com/spark/agent/service/DiagnosisPromptBuilderTest.java`

**Interfaces:**
- Produces: `DiagnosisPromptBuilder.systemPrompt(): String`, `DiagnosisPromptBuilder.structureSystemPrompt(): String`, `DiagnosisPromptBuilder.userPrompt(AlertRecord alert): String` — consumed by Task 2.

- [ ] **Step 1: Write the failing tests**

```java
package com.spark.agent.service;

import com.spark.agent.entity.AlertRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosisPromptBuilderTest {

    private DiagnosisPromptBuilder builder;
    private AlertRecord alert;

    @BeforeEach
    void setUp() {
        builder = new DiagnosisPromptBuilder();

        alert = new AlertRecord();
        alert.setDeviceKey("DK_TEST");
        alert.setIdentifier("temperature");
        alert.setTriggerValue("150.5");
        alert.setLevel((short) 2);
        alert.setAlertContent("High temperature alert");
        alert.setTriggerTime(LocalDateTime.of(2026, 7, 3, 10, 30));
    }

    @Test
    void userPrompt_includesAllAlertFields() {
        String prompt = builder.userPrompt(alert);

        assertTrue(prompt.contains("DK_TEST"));
        assertTrue(prompt.contains("temperature"));
        assertTrue(prompt.contains("150.5"));
        assertTrue(prompt.contains("2"));
        assertTrue(prompt.contains("High temperature alert"));
        assertTrue(prompt.contains("2026-07-03T10:30"));
    }

    @Test
    void systemPrompt_mentionsAvailableTools() {
        String prompt = builder.systemPrompt();

        assertFalse(prompt.isBlank());
        assertTrue(prompt.contains("queryDeviceStatus"));
        assertTrue(prompt.contains("queryDeviceHistory"));
        assertTrue(prompt.contains("queryDeviceAlerts"));
        assertTrue(prompt.contains("queryDeviceManual"));
    }

    @Test
    void structureSystemPrompt_requestsStructuredFields() {
        String prompt = builder.structureSystemPrompt();

        assertFalse(prompt.isBlank());
        assertTrue(prompt.contains("rootCause"));
        assertTrue(prompt.contains("suggestion"));
        assertTrue(prompt.contains("confidence"));
        assertTrue(prompt.contains("diagnosisDetail"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisPromptBuilderTest"`
Expected: FAIL — compile error, `DiagnosisPromptBuilder` does not exist yet.

- [ ] **Step 3: Write the implementation**

```java
package com.spark.agent.service;

import com.spark.agent.entity.AlertRecord;
import org.springframework.stereotype.Component;

@Component
public class DiagnosisPromptBuilder {

    private static final String SYSTEM_PROMPT = """
            You are an expert industrial IoT diagnosis assistant for factory equipment.
            You have tools available to investigate an alert: queryDeviceStatus (device info
            and latest telemetry), queryDeviceHistory (historical telemetry for one
            identifier), queryDeviceAlerts (past alerts for the device), and queryDeviceManual
            (search equipment manuals by device model and question/symptom).
            Use these tools as needed to gather the context you need — call queryDeviceStatus
            first if you need the device's product model to search its manual. Then determine
            the most likely root cause and a concrete, actionable remediation. Be specific and
            concise.
            """;

    /**
     * Structured output is requested in a separate, tool-free follow-up call rather than
     * on the tool-calling call itself: small local models reliably drift into free-form
     * prose once tool results are in context, which breaks JSON parsing of the entity()
     * response. Asking a plain formatting question against the finished investigation is
     * a much easier task and parses far more reliably.
     */
    private static final String STRUCTURE_SYSTEM_PROMPT = """
            Extract the diagnosis below into the required structured fields: rootCause (concise
            root cause), suggestion (concrete, actionable remediation), confidence (integer 0-100
            reflecting how certain the diagnosis is), diagnosisDetail (the full diagnosis
            narrative). Do not invent information beyond what's in the diagnosis.
            """;

    public String systemPrompt() {
        return SYSTEM_PROMPT;
    }

    public String structureSystemPrompt() {
        return STRUCTURE_SYSTEM_PROMPT;
    }

    public String userPrompt(AlertRecord alert) {
        return """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                Investigate this alert using the available tools as needed, then give your diagnosis.
                """.formatted(alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(),
                        alert.getLevel(), alert.getAlertContent(), alert.getTriggerTime());
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisPromptBuilderTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/service/DiagnosisPromptBuilder.java src/test/java/com/spark/agent/service/DiagnosisPromptBuilderTest.java
git commit -m "feat(diagnosis): add DiagnosisPromptBuilder with unit tests"
```

---

### Task 2: Wire DiagnosisPromptBuilder into DiagnosisAgentService

**Files:**
- Modify: `src/main/java/com/spark/agent/service/DiagnosisAgentService.java`
- Modify: `src/test/java/com/spark/agent/service/DiagnosisAgentServiceTest.java`

**Interfaces:**
- Consumes: `DiagnosisPromptBuilder.systemPrompt()`, `DiagnosisPromptBuilder.structureSystemPrompt()`, `DiagnosisPromptBuilder.userPrompt(AlertRecord)` from Task 1.

- [ ] **Step 1: Update the failing/changed test setup first**

Edit `DiagnosisAgentServiceTest.java`: add a real `DiagnosisPromptBuilder` field and pass it into the service constructor.

```java
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
```

(Only the `setUp` method changes; no other test method changes.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisAgentServiceTest"`
Expected: FAIL — compile error, `DiagnosisAgentService` constructor does not yet accept a `DiagnosisPromptBuilder` argument.

- [ ] **Step 3: Modify DiagnosisAgentService to delegate to the builder**

Remove the `SYSTEM_PROMPT` and `STRUCTURE_SYSTEM_PROMPT` constants (lines 26–50 in the current file) and the field-level javadoc comment that describes `STRUCTURE_SYSTEM_PROMPT` (it moves with the constant into `DiagnosisPromptBuilder`, already present there from Task 1). Add the new field, and replace the inline prompt usages:

```java
    private final AlertRecordRepository alertRecordRepository;
    private final ChatClient.Builder chatClientBuilder;
    private final ToolCallbackProvider deviceToolCallbacks;
    private final AppProperties appProperties;
    private final DiagnosisPromptBuilder promptBuilder;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    @Transactional
    public DiagnosisResult diagnose(Long alertId) {
        AlertRecord alert = alertRecordRepository.findById(alertId)
                .orElseThrow(() -> new EntityNotFoundException(
                        "AlertRecord " + alertId + " not found for diagnosis"));

        String userPrompt = promptBuilder.userPrompt(alert);

        DiagnosisResult result = runInference(userPrompt);

        writeBack(alert, result);
        return result;
    }

    private DiagnosisResult runInference(String userPrompt) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                String investigation = chatClient.prompt()
                        .system(promptBuilder.systemPrompt())
                        .tools(deviceToolCallbacks)
                        .user(userPrompt)
                        .call()
                        .content();
                return chatClient.prompt()
                        .system(promptBuilder.structureSystemPrompt())
                        .user(investigation)
                        .call()
                        .entity(DiagnosisResult.class);
            }).orTimeout(150, TimeUnit.SECONDS).join();
        } catch (Exception e) {
            log.error("[Diagnosis] LLM inference failed or timed out: {}", e.getMessage());
            return new DiagnosisResult("", "", 0, "Inference failed: " + e.getMessage());
        }
    }
```

`writeBack(...)` is unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisAgentServiceTest" --tests "com.spark.agent.service.DiagnosisPromptBuilderTest"`
Expected: PASS (all tests in both classes)

- [ ] **Step 5: Run the full build to confirm no other callers broke**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL (no other file constructs `DiagnosisAgentService` directly — Spring wires it via `@RequiredArgsConstructor`/DI in production).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/spark/agent/service/DiagnosisAgentService.java src/test/java/com/spark/agent/service/DiagnosisAgentServiceTest.java
git commit -m "refactor(diagnosis): delegate prompt construction to DiagnosisPromptBuilder"
```
