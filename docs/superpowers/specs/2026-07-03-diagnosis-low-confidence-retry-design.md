# Low-Confidence Diagnosis Retry (P3 task 020)

## Goal

When a diagnosis comes back with confidence below a threshold, automatically retry once with
an explicit instruction to widen the historical investigation window, and keep whichever
result is better.

## Non-goals

- No change to the diagnosis tool set or the structured-output contract (`DiagnosisResult`).
- No more than one retry — this is a single extra attempt, not a retry loop.
- No change to the existing `diagnosisConfidenceThreshold` (80%) that decides
  auto-diagnosed vs. human-review-required.

## Configuration

`AppProperties` gets a new field:

```java
/** confidence (0-100) below which diagnose() retries once with a wider history window */
private int diagnosisRetryConfidenceThreshold = 40;
```

Config key: `app.diagnosis-retry-confidence-threshold` (default `40`), added to
`application.yaml` immediately after the existing `app.diagnosis-confidence-threshold: 80`
entry.

## DiagnosisPromptBuilder changes

Add one new method:

```java
public String retryUserPrompt(AlertRecord alert)
```

Same alert fields as `userPrompt(alert)`, plus an explicit instruction that this is a
low-confidence retry and the LLM should call `queryDeviceHistory` with `hours=2` (120
minutes) to gather a wider window of telemetry before finalizing its diagnosis.

## DiagnosisAgentService changes

In `diagnose(Long alertId)`, after the first `runInference(promptBuilder.userPrompt(alert))`:

```
if (result.confidence() < appProperties.getDiagnosisRetryConfidenceThreshold()) {
    DiagnosisResult retryResult = runInference(promptBuilder.retryUserPrompt(alert));
    if (retryResult.confidence() > result.confidence()) {
        result = retryResult;
    }
}
```

- Retry fires at most once per `diagnose()` call.
- "Better" = strictly higher confidence; a tie keeps the original (first) result.
- `writeBack(alert, result)` is unchanged — it always operates on the final chosen result.
- `runInference`'s existing timeout/error handling is reused unmodified for the retry call
  (a failed retry just returns a confidence-0 result, which loses the "better" comparison
  and the original result is kept).

## Testing

In `DiagnosisAgentServiceTest`:
- High confidence (≥ threshold): no retry — `chatClient.prompt()` invoked exactly twice total
  (one investigation call + one structuring call).
- Low confidence (< threshold): exactly one retry — `chatClient.prompt()` invoked exactly four
  times total (two investigation+structuring round trips).
- The retry's `.user(...)` call receives the `retryUserPrompt` content (assert via captured
  argument, distinguishing it from the first `userPrompt` content).
- When the retry produces a higher confidence than the first attempt, the higher-confidence
  result is the one written back via `alertRecordRepository.save(...)`.
- When the retry produces an equal or lower confidence, the original (first) result is written
  back.

## Acceptance criteria

Confidence < 40% automatically triggers exactly one retry with a 120-minute historical window
instruction; the final written-back result is whichever of the two has higher confidence.
