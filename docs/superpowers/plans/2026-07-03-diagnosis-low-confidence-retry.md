# Low-Confidence Diagnosis Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When `DiagnosisAgentService.diagnose()` gets a confidence below a configurable threshold (default 40), retry once with an explicit instruction to widen the historical investigation window to 120 minutes, and keep whichever of the two results has higher confidence.

**Architecture:** Add `DiagnosisPromptBuilder.retryUserPrompt(alert)` producing the same alert context as `userPrompt(alert)` plus a widen-the-window instruction. `DiagnosisAgentService.diagnose()` checks the first result's confidence against a new `AppProperties.diagnosisRetryConfidenceThreshold` (default 40); below it, runs `runInference` a second time with the retry prompt and keeps the higher-confidence result (ties keep the first).

**Tech Stack:** Java 25, Spring Boot 4.1.0, JUnit 5, Mockito.

## Global Constraints

- Retry fires at most once per `diagnose()` call — never a loop.
- "Better" = strictly higher confidence; a tie keeps the original (first) result.
- No change to `diagnosisConfidenceThreshold` (80) or the auto-diagnosed/human-review decision logic in `writeBack`.
- `runInference`'s existing timeout/error handling is reused unmodified for the retry call.

---

### Task 1: Add DiagnosisPromptBuilder.retryUserPrompt with unit tests

**Files:**
- Modify: `src/main/java/com/spark/agent/service/DiagnosisPromptBuilder.java`
- Modify: `src/test/java/com/spark/agent/service/DiagnosisPromptBuilderTest.java`

**Interfaces:**
- Produces: `DiagnosisPromptBuilder.retryUserPrompt(AlertRecord alert): String` — consumed by Task 2.

- [ ] **Step 1: Write the failing test**

Add to `DiagnosisPromptBuilderTest.java` (inside the existing class, alongside `userPrompt_includesAllAlertFields`):

```java
    @Test
    void retryUserPrompt_includesAllAlertFieldsAndWidenWindowInstruction() {
        String prompt = builder.retryUserPrompt(alert);

        assertTrue(prompt.contains("DK_TEST"));
        assertTrue(prompt.contains("temperature"));
        assertTrue(prompt.contains("150.5"));
        assertTrue(prompt.contains("High temperature alert"));
        assertTrue(prompt.contains("2026-07-03T10:30"));
        assertTrue(prompt.contains("hours=2"));
        assertTrue(prompt.contains("120"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisPromptBuilderTest"`
Expected: FAIL — compile error, `retryUserPrompt` does not exist on `DiagnosisPromptBuilder`.

- [ ] **Step 3: Write the implementation**

In `DiagnosisPromptBuilder.java`, add this method after `userPrompt`:

```java
    public String retryUserPrompt(AlertRecord alert) {
        return """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                This is a retry: the previous investigation produced a low-confidence diagnosis.
                Widen your investigation — when calling queryDeviceHistory, use hours=2 (120
                minutes) to capture more historical context — then give your diagnosis.
                """.formatted(alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(),
                        alert.getLevel(), alert.getAlertContent(), alert.getTriggerTime());
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisPromptBuilderTest"`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/service/DiagnosisPromptBuilder.java src/test/java/com/spark/agent/service/DiagnosisPromptBuilderTest.java
git commit -m "feat(diagnosis): add DiagnosisPromptBuilder.retryUserPrompt for low-confidence retries"
```

---

### Task 2: Wire low-confidence retry into DiagnosisAgentService

**Files:**
- Modify: `src/main/java/com/spark/agent/config/AppProperties.java`
- Modify: `src/main/resources/application.yaml:146-147`
- Modify: `src/main/java/com/spark/agent/service/DiagnosisAgentService.java`
- Modify: `src/test/java/com/spark/agent/service/DiagnosisAgentServiceTest.java`

**Interfaces:**
- Consumes: `DiagnosisPromptBuilder.retryUserPrompt(AlertRecord)` from Task 1.
- Produces: `AppProperties.getDiagnosisRetryConfidenceThreshold(): int` (Lombok `@Data` getter).

- [ ] **Step 1: Write the failing tests**

Add to `DiagnosisAgentServiceTest.java`, after `diagnose_lowConfidence_writesHumanReviewStatus`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisAgentServiceTest"`
Expected: FAIL — `diagnose_lowConfidence_retriesOnceWithWiderWindow` fails because `chatClient.prompt()` is only invoked twice (no retry logic yet); `diagnose_retryProducesHigherConfidence_writesBackRetryResult` fails because no retry happens so `result.confidence()` is `35`, not `70`.

- [ ] **Step 3: Add the config field**

In `AppProperties.java`, add after `diagnosisConfidenceThreshold`:

```java
    /** confidence (0-100) below which diagnose() retries once with a wider (120-min) history window */
    private int diagnosisRetryConfidenceThreshold = 40;
```

In `application.yaml`, add after line 147 (`diagnosis-confidence-threshold: 80`):

```yaml
  # below this confidence, diagnose() retries once with a wider (120-min) history window
  diagnosis-retry-confidence-threshold: 40
```

- [ ] **Step 4: Add the retry logic to DiagnosisAgentService**

Replace the body of `diagnose(Long alertId)`:

```java
    @Transactional
    public DiagnosisResult diagnose(Long alertId) {
        AlertRecord alert = alertRecordRepository.findById(alertId)
                .orElseThrow(() -> new EntityNotFoundException(
                        "AlertRecord " + alertId + " not found for diagnosis"));

        DiagnosisResult result = runInference(promptBuilder.userPrompt(alert));

        if (result.confidence() < appProperties.getDiagnosisRetryConfidenceThreshold()) {
            DiagnosisResult retryResult = runInference(promptBuilder.retryUserPrompt(alert));
            if (retryResult.confidence() > result.confidence()) {
                result = retryResult;
            }
        }

        writeBack(alert, result);
        return result;
    }
```

(`runInference` and `writeBack` are unchanged.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.DiagnosisAgentServiceTest" --tests "com.spark.agent.service.DiagnosisPromptBuilderTest"`
Expected: PASS (all tests in both classes, including the pre-existing `diagnose_lowConfidence_writesHumanReviewStatus`, which now exercises the retry path internally but still ends with `diagnosisStatus == 1` since both attempts return confidence 35)

- [ ] **Step 6: Run the full build**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/config/AppProperties.java src/main/resources/application.yaml src/main/java/com/spark/agent/service/DiagnosisAgentService.java src/test/java/com/spark/agent/service/DiagnosisAgentServiceTest.java
git commit -m "feat(diagnosis): retry once with wider history window on low-confidence diagnosis"
```
