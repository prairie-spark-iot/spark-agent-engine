# Diagnosis Prompt Builder (P3 task 019)

## Goal

Separate prompt-construction logic from `DiagnosisAgentService` so it can be unit tested
without a `ChatClient`, and so future prompt changes (e.g. task 020's retry-with-wider-window
logic) have a single place to live.

## Non-goals

- No behavior change to the diagnosis flow itself.
- No change to `DiagnosisResult` or the structured-output contract.

## New component

`com.spark.agent.service.DiagnosisPromptBuilder` — Spring `@Component`, no dependencies.

```java
@Component
public class DiagnosisPromptBuilder {
    public String systemPrompt();                    // investigation system prompt
    public String structureSystemPrompt();            // structured-extraction system prompt
    public String userPrompt(AlertRecord alert);       // per-alert investigation prompt
}
```

Content of all three is moved verbatim from the current `SYSTEM_PROMPT` /
`STRUCTURE_SYSTEM_PROMPT` constants and the inline `.formatted(...)` block in
`DiagnosisAgentService.diagnose()`.

## DiagnosisAgentService changes

- Add `DiagnosisPromptBuilder promptBuilder` as a new `final` field — picked up automatically
  by the existing `@RequiredArgsConstructor`.
- Remove the two prompt constants and the inline user-prompt formatting.
- Replace call sites:
  - `.system(SYSTEM_PROMPT)` → `.system(promptBuilder.systemPrompt())`
  - `.system(STRUCTURE_SYSTEM_PROMPT)` → `.system(promptBuilder.structureSystemPrompt())`
  - inline `userPrompt` block → `promptBuilder.userPrompt(alert)`

## Testing

- New `DiagnosisPromptBuilderTest`: plain JUnit, no Mockito, no ChatClient. Builds a sample
  `AlertRecord`, asserts `userPrompt(alert)` contains the alert's device key, identifier,
  trigger value, level, content, and trigger time; asserts both system prompts are non-blank
  and contain their key instructional phrases.
- `DiagnosisAgentServiceTest`: pass a real `new DiagnosisPromptBuilder()` into the service
  constructor (it has no dependencies of its own) — no mocking needed since the tests don't
  assert on prompt text.

## Acceptance criteria

Prompt-building logic is independently unit-testable without a ChatClient dependency;
`DiagnosisAgentService` behavior is unchanged (all existing tests pass).
