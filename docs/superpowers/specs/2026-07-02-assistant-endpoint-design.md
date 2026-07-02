# Natural-Language Ops Assistant (Phase 4.3.3) — Design

## Purpose

Add `POST /api/assistant { "question": "2号注塑机最近有什么异常" }`: an operator asks a plain-language question, the LLM autonomously decides which device-query tools to call to investigate, and replies in natural language. This reuses the tool-calling agent infrastructure built for 4.3.1 (`DeviceMcpToolService`, `deviceToolCallbacks`) and proven in 4.3.2 (`DiagnosisAgentService`'s rewrite to `ChatClient.prompt().tools(...)`), rather than building a new agent loop from scratch.

**Gap found during design:** all 4 existing tools (`queryDeviceStatus`, `queryDeviceHistory`, `queryDeviceAlerts`, `queryDeviceManual`) require an exact `deviceKey` (e.g. `DK_INJ_002`) as input. Real device names in the DB are like `injection_02`, `compressor_01` — there is no tool that lets the LLM resolve a natural-language reference ("2号注塑机") to a `deviceKey`. A 5th tool, `listDevices`, closes this gap.

## Components

| File | Change |
|---|---|
| `repository/DeviceRepository.java` | Add `List<Device> findByDeleted(Short deleted)` |
| `dto/DeviceSummary.java` | New record: `(String deviceKey, String deviceName, String productKey, boolean online)` |
| `mcp/DeviceMcpToolService.java` | Add `@Tool listDevices()` — lists all non-deleted devices with key, name, product model, online status |
| `service/AssistantService.java` | New. Mirrors `DiagnosisAgentService`'s `ChatClient` setup; single `answer(String question)` method returning free-form text |
| `controller/AssistantController.java` | New. `POST /api/assistant`, inline request record, `R<String>` response — mirrors `RagController` |
| `service/AssistantServiceTest.java` | New unit test, mirrors `DiagnosisAgentServiceTest` (mocked `ChatClient`) |

No changes to `McpToolConfig` — `deviceToolCallbacks` wraps the whole `DeviceMcpToolService` instance already, so the new tool is picked up automatically, both for the diagnosis agent and for this new assistant, and exposed over the real MCP server (`spring-ai-starter-mcp-server-webmvc`) with no extra wiring.

## New Tool: `listDevices`

```java
@Tool(description = "List all devices with their key, display name, product model, and online " +
        "status — call this first when the question refers to a device by name or description " +
        "rather than its exact deviceKey")
public List<DeviceSummary> listDevices() {
    log.debug("[MCP Tool] listDevices");
    return deviceRepository.findByDeleted((short) 0).stream()
            .map(d -> new DeviceSummary(
                    d.getDeviceKey(),
                    d.getDeviceName(),
                    productRepository.findById(d.getProductId()).map(Product::getProductKey).orElse(null),
                    d.getOnlineStatus() == 1))
            .toList();
}
```

No pagination/limit param — device count in this deployment is small (6 devices at time of writing); the LLM filters by name itself from the full list. Same `deleted = 0` convention as the other tools.

## `AssistantService`

```java
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

    public String answer(String question) {
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
```

Deliberate differences from `DiagnosisAgentService`:
- **No `@Transactional`.** Diagnosis needs it because `diagnose()` also writes the result back to `AlertRecord` via managed-entity dirty checking. The assistant makes no writes; each tool call opens its own short-lived read. Wrapping the whole multi-round-trip LLM call (which can take up to 60s) in a transaction would hold a pooled DB connection open for the duration for no benefit.
- **No try/catch.** Diagnosis catches broadly because it must always produce a non-null `DiagnosisResult` to write back, even on failure. The assistant has no such constraint — on timeout or LLM failure, `CompletableFuture.join()` throws an unchecked `CompletionException`, which propagates through the controller to `GlobalExceptionHandler`'s existing catch-all `Exception` handler (`common/GlobalExceptionHandler.java:64-69`), returning `R.fail(500, "Internal server error: ...")`. No new exception-handling code needed.
- **`.content()` not `.entity(DiagnosisResult.class)`.** Output is a free-form natural-language answer, not a structured record.

## `AssistantController`

```java
@RestController
@RequestMapping("/api/assistant")
@RequiredArgsConstructor
public class AssistantController {

    private final AssistantService assistantService;

    record AssistantRequest(@NotBlank String question) {}

    @PostMapping
    public R<String> ask(@RequestBody @Valid AssistantRequest req) {
        return R.ok(assistantService.answer(req.question()));
    }
}
```

`@NotBlank` on `question` triggers the existing `MethodArgumentNotValidException` handler (`GlobalExceptionHandler.java:46-55`) for empty/missing input → `400`. No new validation plumbing.

## Data Flow

```
POST /api/assistant {"question": "2号注塑机最近有什么异常"}
  └─► AssistantController.ask()
        └─► AssistantService.answer(question)
              └─► ChatClient.prompt()
                    .system(SYSTEM_PROMPT)
                    .tools(deviceToolCallbacks)   ← same bean as DiagnosisAgentService,
                    .user(question)                 now including listDevices
                    .call().content()
                       ⟲ tool-calling loop (Spring AI / qwen2.5:7b, framework-managed):
                         e.g. listDevices() → resolve "2号注塑机" to DK_INJ_002
                              → queryDeviceAlerts(DK_INJ_002) and/or queryDeviceHistory(...)
                              → model composes final NL answer
              └─► returns String
        └─► R.ok(answer)
  └─► 200 {"code":0,"msg":"success","data":"2号注塑机最近1小时内..."}
```

## Response Format

`R<String>` — the standard `{code, msg, data}` wrapper already used by every other endpoint, with `data` being the raw answer string. No tool-call trace or timing metadata in the response: Spring AI's `ChatClient` doesn't expose per-call tool-invocation introspection without a custom `Advisor`, and that's out of scope for this phase. Tool-call visibility for debugging/demo purposes comes from the existing `log.debug("[MCP Tool] ...")` lines in `DeviceMcpToolService` (added in 4.3.2, now covering 5 tools instead of 4).

## Error Handling

- Blank/missing `question` → `400` via existing `@Valid`/`@NotBlank` + `MethodArgumentNotValidException` handler.
- LLM/tool-calling failure or 60s timeout → unchecked exception propagates to the existing catch-all `Exception` handler → `500`.
- A tool call for an unknown/misspelled device key returns `found=false` or an empty list (existing 4.3.1 convention, unchanged) — the LLM is expected to relay "device not found" in its natural-language answer rather than the request failing.

## Testing

- Unit test `AssistantServiceTest`, mocking `ChatClient`/`ChatClient.Builder` the same way `DiagnosisAgentServiceTest` does, verifying `answer()` returns the mocked content and that `.tools(deviceToolCallbacks)` is invoked.
- Controller test for `AssistantController` verifying the `R<String>` wrapping and the 400 path for a blank question, following `ApiControllerTest`'s conventions.
- Manual end-to-end verification: start the app, POST the exact demo question (`2号注塑机最近有什么异常`) against the running stack, confirm the answer correctly names `DK_INJ_002`/`injection_02` and reflects real alert/telemetry data, and tail `com.spark.agent.mcp` at DEBUG to confirm `listDevices` was actually called before the device-specific tools.

## Out of Scope

- Tool-call trace / metadata in the response (no `Advisor`-based introspection).
- Multi-turn conversation / follow-up questions — each request is stateless, single-shot (no session or history storage).
- Auth/access control — same accepted tradeoff as 4.3.1/4.3.2, no auth on this endpoint either.
- Fleet-wide aggregate tools beyond `listDevices` (e.g. "how many devices are currently in alarm") — answerable today by the LLM combining `listDevices` + `queryDeviceAlerts` per device, no new tool needed for the stated demo scope.
