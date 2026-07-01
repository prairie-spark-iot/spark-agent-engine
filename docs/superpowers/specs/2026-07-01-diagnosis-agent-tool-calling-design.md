# Diagnosis Agent Tool-Calling Rewrite (Phase 4.3.2) — Design

## Purpose

Today, `DiagnosisAgentService.diagnose()` gathers all context (device info, latest telemetry, alert history, manual excerpts) up front in Java, hands it to the LLM in one shot, and — if confidence is low or no manuals were found — retries once with a wider telemetry window ("reflection"). This is a fixed, hardcoded investigation strategy chosen by the developer, not the agent.

This phase turns it into a true tool-calling agent: the LLM decides for itself, per alert, which of the 4 MCP tools built in 4.3.1 (`queryDeviceStatus`, `queryDeviceHistory`, `queryDeviceAlerts`, `queryDeviceManual`) to call, how many times, and in what order, before producing its final diagnosis. The reflection retry mechanism is removed — an agent that can pull more telemetry itself when it needs it doesn't need a hardcoded second pass.

## Components

| File | Change |
|---|---|
| `service/DiagnosisAgentService.java` | Rewrite `diagnose()`: drop manual context-gathering (device/telemetry/history/manuals) and the reflection retry; keep Kafka payload parsing + managed-entity refetch + `writeBack`. `runInference` becomes a single `ChatClient.prompt()` call with `.toolCallbacks(deviceToolCallbacks)`. |
| `config/AppProperties.java` | Delete `diagnosisReflectionConfidenceThreshold` and `diagnosisReflectionTelemetryWindowMinutes` fields. Keep `diagnosisConfidenceThreshold`. |
| `application.yaml` | Delete the two `diagnosis-reflection-*` keys under `app:`. Keep `diagnosis-confidence-threshold`. |
| `mcp/DeviceMcpToolService.java` | Add one `log.debug(...)` line per `@Tool` method (4 total), logging the method name and its input parameter(s). No behavior change. Requires adding `@Slf4j`. |
| `mcp/McpToolConfig.java` | No change — `deviceToolCallbacks` bean from 4.3.1 is reused as-is. |

No new files. No changes to `AlertTriggeredConsumer`, `writeBack`, `DiagnosisResult`, or any repository.

## Rewritten `diagnose()`

```java
@Transactional
public DiagnosisResult diagnose(String alertMessage) {
    AlertRecord kafkaAlert;
    try {
        kafkaAlert = objectMapper.readValue(alertMessage, AlertRecord.class);
    } catch (Exception e) {
        log.error("[Diagnosis] Failed to parse alert message: {}", e.getMessage());
        return null;
    }

    AlertRecord alert = alertRecordRepository.findById(kafkaAlert.getId())
            .orElseThrow(() -> new EntityNotFoundException(
                    "AlertRecord " + kafkaAlert.getId() + " not found for diagnosis"));

    String userPrompt = """
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

    DiagnosisResult result = chatClient.prompt()
            .system(SYSTEM_PROMPT)
            .toolCallbacks(deviceToolCallbacks)
            .user(userPrompt)
            .call()
            .entity(DiagnosisResult.class);

    writeBack(alert, result);
    return result;
}
```

`deviceToolCallbacks` (the `ToolCallbackProvider` bean from `McpToolConfig`) is injected as a new constructor dependency, replacing `deviceRepository`, `productRepository`, `deviceDataRepository`, and `ragSearchService` — all four become unused in this class and are removed from the constructor and imports. `alertRecordRepository` stays (still needed for refetch + `writeBack`).

`SYSTEM_PROMPT` is extended with tool-usage guidance:

```java
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
```

## Data Flow

```
AlertTriggeredConsumer (Kafka) → DiagnosisAgentService.diagnose(alertMessage)
  ├─► parse Kafka payload → re-fetch managed AlertRecord (unchanged)
  ├─► ChatClient.prompt()
  │     .system(SYSTEM_PROMPT + tool-usage guidance)
  │     .toolCallbacks(deviceToolCallbacks)   ← same bean McpToolConfig registers for external MCP clients
  │     .user(raw alert fields only)
  │     .call().entity(DiagnosisResult.class)
  │        ⟲ tool-calling loop: model calls queryDeviceStatus / queryDeviceHistory /
  │          queryDeviceAlerts / queryDeviceManual zero or more times, on its own
  │          judgment, before producing a final answer
  └─► writeBack(alert, result)   (unchanged)
```

## Config Cleanup

`diagnosisReflectionConfidenceThreshold` and `diagnosisReflectionTelemetryWindowMinutes` become dead once the reflection retry is deleted — removed from `AppProperties` and their `application.yaml` entries under `app:`, rather than left as unused config. `diagnosisConfidenceThreshold` is unchanged; it still gates `writeBack`'s auto-diagnosed vs. human-review-required split.

## Error Handling

Unchanged from today: Kafka payload parse failure → log + return null (no diagnosis attempted); `AlertRecord` not found in Postgres → `EntityNotFoundException`, same as now.

New consideration: if the tool-calling loop fails entirely (e.g. Ollama down, or the model never produces a final non-tool-call response), `.call().entity(...)` throws — this propagates up through the existing `@Transactional` method exactly as a failure in today's single-shot `runInference` would (uncaught, visible in the Kafka consumer's logs). No new handling needed; this isn't a new failure mode, just an existing one with more steps leading to it.

Expected behavior change, not a regression: diagnosis latency increases, since the agent may make several LLM round-trips per alert instead of one. `AlertTriggeredConsumer` already runs with concurrency across partitions specifically so one alert's LLM-bound processing doesn't block the others — this was already the rationale for that setting, and holds equally well with more round-trips per alert.

## Observability

`DeviceMcpToolService`'s four `@Tool` methods are currently silent passthroughs — there's no way to see whether the agent investigated anything before answering. Adding one `log.debug(...)` line per method (parameters only, no return value) so a log tail during verification shows the real tool-call sequence chosen by the agent:

```java
log.debug("[MCP Tool] queryDeviceStatus deviceKey={}", deviceKey);
log.debug("[MCP Tool] queryDeviceHistory deviceKey={} identifier={} hours={}", deviceKey, identifier, hours);
log.debug("[MCP Tool] queryDeviceAlerts deviceKey={} limit={}", deviceKey, limit);
log.debug("[MCP Tool] queryDeviceManual deviceModel={} question={}", deviceModel, question);
```

This applies equally to external MCP clients and to the diagnosis agent's own calls — same tool service, same log lines either way.

## Testing

Same manual-verification posture as 4.3.1 (no test scaffolding exists for services in this project): trigger a real alert end-to-end via the existing MQTT→Kafka pipeline, then:
- tail logs at DEBUG for `com.spark.agent.mcp` to confirm the agent actually invoked tools (and in what sequence) rather than answering blind,
- inspect the written-back `AlertRecord` row (`root_cause`, `suggestion`, `confidence`, `diagnosis_detail`, `diagnosis_status`) for a sensible result.

Flagged as an open risk, not a blocker: qwen2.5:7b's tool-calling reliability at 7B scale is unproven in this codebase — the first real run is the actual test.

## Out of Scope

- No changes to `DeviceMcpToolService`'s tool logic, signatures, or the 4.3.1-established error-handling conventions (device-not-found returns `found=false`, not an exception; unknown keys return empty lists) — only the new debug log lines are added.
- No auth/access control changes — same accepted tradeoff as 4.3.1.
- No changes to `writeBack`'s confidence-threshold logic or `diagnosisConfidenceThreshold`.
- No new REST/MCP endpoints.
