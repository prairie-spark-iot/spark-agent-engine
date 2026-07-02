# Natural-Language Ops Assistant (4.3.3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /api/assistant` so an operator can ask a plain-language question (e.g. "2号注塑机最近有什么异常") and get a natural-language answer, with the LLM autonomously calling device-query tools to investigate.

**Architecture:** Reuse the tool-calling `ChatClient` pattern already proven in `DiagnosisAgentService` (Spring AI + Ollama qwen2.5:7b + `deviceToolCallbacks`). Add one new MCP tool (`listDevices`) to close the device-name-to-deviceKey resolution gap, a new stateless `AssistantService`, and a new `AssistantController`.

**Tech Stack:** Java 25, Spring Boot 4.1.0, Spring AI 2.0.0 (`ChatClient`, `@Tool`/`ToolCallbackProvider`), Spring Data JPA, JUnit 5 + Mockito.

## Global Constraints

- Package root: `com.spark.agent`. New files follow existing package layout (`repository/`, `dto/`, `mcp/`, `service/`, `controller/`).
- All REST responses use `R<T>` (`common/R.java`): `R.ok(data)` for success, exceptions for failure (handled centrally by `common/GlobalExceptionHandler.java` — do not add local try/catch that duplicates it).
- `deleted` is a `Short` column; existing convention throughout the codebase is `(short) 0` for "not deleted" — match this exactly, do not introduce a different literal form.
- No new Gradle dependencies — Spring AI, Mockito, and JUnit 5 are already on the classpath.
- Do not modify `DiagnosisAgentService`, `McpToolConfig`, or any 4.3.1/4.3.2 tool signatures — this plan is additive only.

---

## Task 1: `listDevices` MCP tool

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/DeviceRepository.java`
- Create: `src/main/java/com/spark/agent/dto/DeviceSummary.java`
- Modify: `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`
- Test: `src/test/java/com/spark/agent/mcp/DeviceMcpToolServiceTest.java` (new file)

**Interfaces:**
- Produces: `DeviceRepository.findByDeleted(Short deleted)` → `List<Device>`
- Produces: `record DeviceSummary(String deviceKey, String deviceName, String productKey, boolean online)`
- Produces: `DeviceMcpToolService.listDevices()` → `List<DeviceSummary>` (no params), picked up automatically by the existing `deviceToolCallbacks` bean in `McpToolConfig` (no change needed there — it wraps the whole `DeviceMcpToolService` instance).

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/spark/agent/mcp/DeviceMcpToolServiceTest.java`:

```java
package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceSummary;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.service.RagSearchService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeviceMcpToolServiceTest {

    @Mock
    private AlertRecordRepository alertRecordRepository;
    @Mock
    private DeviceRepository deviceRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private DeviceDataRepository deviceDataRepository;
    @Mock
    private RagSearchService ragSearchService;

    @InjectMocks
    private DeviceMcpToolService service;

    @Test
    void listDevices_mapsDeviceAndProductFields() {
        Device device = new Device();
        device.setDeviceKey("DK_INJ_002");
        device.setDeviceName("injection_02");
        device.setProductId(50L);
        device.setOnlineStatus((short) 1);

        Product product = new Product();
        product.setProductKey("PK_INJECTION_MA");

        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of(device));
        when(productRepository.findById(50L)).thenReturn(Optional.of(product));

        List<DeviceSummary> result = service.listDevices();

        assertEquals(1, result.size());
        DeviceSummary summary = result.get(0);
        assertEquals("DK_INJ_002", summary.deviceKey());
        assertEquals("injection_02", summary.deviceName());
        assertEquals("PK_INJECTION_MA", summary.productKey());
        assertTrue(summary.online());
    }

    @Test
    void listDevices_offlineDevice_reportsOnlineFalse() {
        Device device = new Device();
        device.setDeviceKey("DK_CMP_001");
        device.setDeviceName("compressor_01");
        device.setProductId(51L);
        device.setOnlineStatus((short) 0);

        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of(device));
        when(productRepository.findById(51L)).thenReturn(Optional.empty());

        List<DeviceSummary> result = service.listDevices();

        assertEquals(1, result.size());
        assertFalse(result.get(0).online());
        assertNull(result.get(0).productKey());
    }

    @Test
    void listDevices_noDevices_returnsEmptyList() {
        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of());

        assertTrue(service.listDevices().isEmpty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.mcp.DeviceMcpToolServiceTest"`
Expected: **compilation failure** — `DeviceSummary` doesn't exist yet and `DeviceMcpToolService.listDevices()` is not defined. This compile error is the expected "red" state for this task (Java's static typing means the test can't even compile against not-yet-written production code).

- [ ] **Step 3: Add `findByDeleted` to `DeviceRepository`**

Edit `src/main/java/com/spark/agent/repository/DeviceRepository.java`, add inside the interface (after `findByDeviceKeyAndDeleted`):

```java
    List<Device> findByDeleted(Short deleted);
```

Add `import java.util.List;` to the existing import block.

- [ ] **Step 4: Create `DeviceSummary` DTO**

Create `src/main/java/com/spark/agent/dto/DeviceSummary.java`:

```java
package com.spark.agent.dto;

public record DeviceSummary(
        String deviceKey,
        String deviceName,
        String productKey,
        boolean online
) {
}
```

- [ ] **Step 5: Add `listDevices` tool method to `DeviceMcpToolService`**

Edit `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`:

Add import:
```java
import com.spark.agent.dto.DeviceSummary;
```

Add method (place before `queryDeviceStatus`, since the LLM should call this first when it doesn't have a deviceKey):

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

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.mcp.DeviceMcpToolServiceTest"`
Expected: PASS (3 tests)

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/repository/DeviceRepository.java \
        src/main/java/com/spark/agent/dto/DeviceSummary.java \
        src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java \
        src/test/java/com/spark/agent/mcp/DeviceMcpToolServiceTest.java
git commit -m "feat(mcp): add listDevices tool for name-based device resolution"
```

---

## Task 2: `AssistantService`

**Files:**
- Create: `src/main/java/com/spark/agent/service/AssistantService.java`
- Test: `src/test/java/com/spark/agent/service/AssistantServiceTest.java` (new file)

**Interfaces:**
- Consumes: `org.springframework.ai.chat.client.ChatClient.Builder` (Spring-provided bean), `org.springframework.ai.tool.ToolCallbackProvider deviceToolCallbacks` (bean from `mcp/McpToolConfig.java`, now including `listDevices` from Task 1)
- Produces: `AssistantService.answer(String question)` → `String` (used by Task 3's controller)

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/spark/agent/service/AssistantServiceTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.service.AssistantServiceTest"`
Expected: **compilation failure** — `AssistantService` doesn't exist yet.

- [ ] **Step 3: Implement `AssistantService`**

Create `src/main/java/com/spark/agent/service/AssistantService.java`:

```java
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.service.AssistantServiceTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/service/AssistantService.java \
        src/test/java/com/spark/agent/service/AssistantServiceTest.java
git commit -m "feat(assistant): add AssistantService using tool-calling ChatClient"
```

---

## Task 3: `AssistantController`

**Files:**
- Create: `src/main/java/com/spark/agent/controller/AssistantController.java`
- Test: `src/test/java/com/spark/agent/controller/AssistantControllerTest.java` (new file)

**Interfaces:**
- Consumes: `AssistantService.answer(String question)` → `String` (from Task 2)
- Produces: `POST /api/assistant` REST endpoint returning `R<String>`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/spark/agent/controller/AssistantControllerTest.java`:

```java
package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.service.AssistantService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AssistantControllerTest {

    @Mock
    private AssistantService assistantService;

    @InjectMocks
    private AssistantController controller;

    @Test
    void ask_returnsAnswerWrappedInR() {
        when(assistantService.answer("2号注塑机最近有什么异常"))
                .thenReturn("2号注塑机最近1小时内触发了1次压力过高告警。");

        R<String> result = controller.ask(new AssistantController.AssistantRequest("2号注塑机最近有什么异常"));

        assertEquals(0, result.getCode());
        assertEquals("success", result.getMsg());
        assertEquals("2号注塑机最近1小时内触发了1次压力过高告警。", result.getData());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.spark.agent.controller.AssistantControllerTest"`
Expected: **compilation failure** — `AssistantController` doesn't exist yet.

- [ ] **Step 3: Implement `AssistantController`**

Create `src/main/java/com/spark/agent/controller/AssistantController.java`:

```java
package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.service.AssistantService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/assistant")
@RequiredArgsConstructor
public class AssistantController {

    private final AssistantService assistantService;

    public record AssistantRequest(@NotBlank String question) {}

    @PostMapping
    public R<String> ask(@RequestBody @Valid AssistantRequest req) {
        return R.ok(assistantService.answer(req.question()));
    }
}
```

Note: `AssistantRequest` is `public` (unlike `RagController`'s package-private inline records) because the test in `Step 1` constructs it directly from the test class in the same package — this matches the record's visibility needs without adding a separate top-level DTO file.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "com.spark.agent.controller.AssistantControllerTest"`
Expected: PASS (1 test)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/spark/agent/controller/AssistantController.java \
        src/test/java/com/spark/agent/controller/AssistantControllerTest.java
git commit -m "feat(assistant): add POST /api/assistant endpoint"
```

---

## Task 4: Full build verification and manual end-to-end check

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL, all tests pass (including the 7 new tests from Tasks 1–3 and all pre-existing tests).

- [ ] **Step 2: Confirm infrastructure is running**

Run: `docker ps --format "{{.Names}}"`
Expected: containers for EMQX, PostgreSQL, Kafka, Redis, and Ollama are up (per `CLAUDE.md`'s Infrastructure table). If Ollama is not containerized in this setup, confirm it separately: `curl -s http://localhost:11434/api/tags`.

- [ ] **Step 3: Start the app**

Run: `./gradlew bootRun --args='--server.port=8081'`
(Use 8081 since CLAUDE.md notes 8080 may already be taken.)
Expected: app starts without errors, log shows Spring context initialized.

- [ ] **Step 4: Send the demo question**

Run:
```bash
curl -s -X POST http://localhost:8081/api/assistant \
  -H "Content-Type: application/json" \
  -d '{"question": "2号注塑机最近有什么异常"}' | jq .
```
Expected: `{"code":0,"msg":"success","data":"<natural-language answer mentioning DK_INJ_002 / injection_02 and real alert data>"}`.

- [ ] **Step 5: Confirm tool-call sequence in logs**

While the app is running with default logging, temporarily check DEBUG output for `com.spark.agent.mcp` (either via `application.yaml`'s logging level or `--logging.level.com.spark.agent.mcp=DEBUG` as a bootRun arg) and confirm the log shows `listDevices` called before any `queryDevice*` call for the same request — this verifies the LLM actually resolved "2号注塑机" via the new tool rather than guessing a deviceKey.

- [ ] **Step 6: Send an edge-case question (blank input)**

Run:
```bash
curl -s -X POST http://localhost:8081/api/assistant \
  -H "Content-Type: application/json" \
  -d '{"question": ""}' | jq .
```
Expected: `{"code":400,"msg":"question: must not be blank","data":null}` (exact message format from `GlobalExceptionHandler.handleValidation`).

- [ ] **Step 7: Stop the app**

Stop the `bootRun` process (Ctrl+C or kill the background process started in Step 3).

No commit for this task — it's verification only, confirming Tasks 1–3's work end-to-end.

---

## Self-Review Notes

- **Spec coverage:** `listDevices` tool (Task 1) ✓, `AssistantService` with no `@Transactional`/no try-catch/`.content()` (Task 2) ✓, `AssistantController` with `R<String>` + `@NotBlank` validation (Task 3) ✓, manual end-to-end verification including the `listDevices`-before-`queryDevice*` log check (Task 4) ✓. Out-of-scope items from the spec (tool-call trace in response, multi-turn history, auth, extra fleet-aggregate tools) are intentionally not tasked.
- **Type consistency checked:** `AssistantService(ChatClient.Builder, ToolCallbackProvider)` constructor signature matches between Task 2's test and implementation; `AssistantController(AssistantService)` via `@RequiredArgsConstructor` matches Task 3's `@InjectMocks` usage; `DeviceSummary(String, String, String, boolean)` field order/names consistent between Task 1's DTO and test assertions.
