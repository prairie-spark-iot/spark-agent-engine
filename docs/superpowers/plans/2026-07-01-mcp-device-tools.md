# MCP Device Tools (Phase 4.3.1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose 4 read-only device-query capabilities (status, history, alerts, manual search) as MCP tools, reusing the same repositories/services the existing REST API (`ApiController`, `RagController`) already uses.

**Architecture:** Add `spring-ai-starter-mcp-server-webmvc` so the MCP server runs embedded inside the existing Spring Boot app on `:8080` (streamable-http transport, default endpoint `/mcp`). One new `@Service` (`DeviceMcpToolService`) holds four `@Tool`-annotated methods; one new `@Configuration` (`McpToolConfig`) wraps it in a `ToolCallbackProvider` bean, which Spring AI's MCP auto-configuration discovers and registers automatically.

**Tech Stack:** Spring Boot 4.1.0, Spring AI 2.0.0 (`spring-ai-starter-mcp-server-webmvc`, MCP streamable-http transport), Gradle 9.5, Java 25.

## Global Constraints

- Read-only tools only — no control/action tools in this phase (spec: "Out of Scope").
- MCP server runs embedded in the existing `:8080` web app via webmvc/streamable-http transport, not stdio (spec: "Transport").
- `queryDeviceHistory` is capped at 500 rows regardless of the `hours` value requested (spec: "queryDeviceHistory").
- `queryDeviceAlerts`' `limit` defaults to 20 when the caller passes `<= 0` (spec: "queryDeviceAlerts").
- `queryDeviceManual`'s `topK` is fixed at 5 internally, not a caller-supplied parameter (spec: "queryDeviceManual").
- No new validation beyond what repositories/services already provide; unknown keys/identifiers yield empty results, not exceptions — except `queryDeviceStatus`, which returns a structured `found=false` result for an unknown `deviceKey` (spec: "Error Handling").
- No changes to `ApiController`, `RagController`, or `DiagnosisAgentService` (spec: "Out of Scope").
- No auth on the MCP endpoint — matches the existing REST API (spec: "Out of Scope").

Confirmed ground truth used throughout this plan (verified directly against the Spring AI 2.0.0 jars on Maven Central, not assumed):
- Dependency coordinate: `org.springframework.ai:spring-ai-starter-mcp-server-webmvc` (covered by the existing `spring-ai-bom:2.0.0` platform import — no version needed on the `implementation` line).
- Config properties (prefix `spring.ai.mcp.server`): `name` (default `mcp-server`), `version` (default `1.0.0`), `type` (enum `SYNC`/`ASYNC`, default `sync`), `protocol` (enum `SSE`/`STREAMABLE`/`STATELESS`, default `streamable`), `streamable-http.mcp-endpoint` (default `/mcp`).
- Tool annotations: `org.springframework.ai.tool.annotation.Tool` (attributes `name`, `description`, `returnDirect`, `resultConverter`) and `org.springframework.ai.tool.annotation.ToolParam` (attributes `required`, `description`).
- Registration API: `org.springframework.ai.tool.method.MethodToolCallbackProvider.builder().toolObjects(Object...).build()` returns a `org.springframework.ai.tool.ToolCallbackProvider`.

Verification data confirmed present in the running Postgres instance (`spark-postgres` container, db `spark_ai`):
- Device `DK_INJ_001` (device_name `injection_01`, product_id `1001` → product_key `PK_INJECTION_MA`), with telemetry rows for identifiers `temperature`, `pressure`, `current`.
- Alert records exist for `DK_INJ_001`/`DK_INJ_002` on identifiers `temperature`/`pressure`.

---

### Task 1: MCP server setup + `queryDeviceStatus`

**Files:**
- Modify: `build.gradle`
- Modify: `src/main/resources/application.yaml`
- Create: `src/main/java/com/spark/agent/dto/DeviceStatusResult.java`
- Create: `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`
- Create: `src/main/java/com/spark/agent/mcp/McpToolConfig.java`

**Interfaces:**
- Produces: `DeviceStatusResult(String deviceKey, boolean found, String deviceName, String productKey, boolean online, LocalDateTime lastOnlineTime, LocalDateTime lastOfflineTime, List<DeviceData> latestTelemetry)` and its static factory `DeviceStatusResult.notFound(String deviceKey)` — used only by this task.
- Produces: `DeviceMcpToolService` class in package `com.spark.agent.mcp` — Tasks 2, 3, 4 add further `@Tool` methods and constructor-injected fields to this same class.
- Consumes (all pre-existing): `DeviceRepository.findByDeviceKeyAndDeleted(String, Short)`, `ProductRepository.findById(Long)` → `Product.getProductKey()`, `DeviceDataRepository.findLatestByDeviceKey(String)`.

- [ ] **Step 1: Add the MCP server dependency**

Edit `build.gradle`, adding the new line directly after the existing Ollama starter:

```groovy
	implementation platform('org.springframework.ai:spring-ai-bom:2.0.0')
	implementation 'org.springframework.ai:spring-ai-starter-model-ollama'
	implementation 'org.springframework.ai:spring-ai-starter-mcp-server-webmvc'
```

- [ ] **Step 2: Add MCP server config**

Edit `src/main/resources/application.yaml`, adding a `mcp:` block as a sibling of the existing `model:` and `ollama:` keys under `spring.ai`:

```yaml
  # ── Spring AI / Ollama ───────────────────────────────────────────────────
  ai:
    model:
      embedding: ollama
      chat: ollama
    ollama:
      base-url: http://localhost:11434
      embedding:
        model: nomic-embed-text
      chat:
        model: qwen2.5:7b
    mcp:
      server:
        name: spark-agent-engine-mcp
        version: 0.0.1-SNAPSHOT
        type: SYNC
```

(`protocol` and `streamable-http.mcp-endpoint` are left at their defaults — `STREAMABLE` and `/mcp` — verified above, so the resolved endpoint will be `http://localhost:8080/mcp`.)

- [ ] **Step 3: Create `DeviceStatusResult`**

Create `src/main/java/com/spark/agent/dto/DeviceStatusResult.java`:

```java
package com.spark.agent.dto;

import com.spark.agent.entity.DeviceData;

import java.time.LocalDateTime;
import java.util.List;

public record DeviceStatusResult(
        String deviceKey,
        boolean found,
        String deviceName,
        String productKey,
        boolean online,
        LocalDateTime lastOnlineTime,
        LocalDateTime lastOfflineTime,
        List<DeviceData> latestTelemetry
) {
    public static DeviceStatusResult notFound(String deviceKey) {
        return new DeviceStatusResult(deviceKey, false, null, null, false, null, null, List.of());
    }
}
```

- [ ] **Step 4: Create `DeviceMcpToolService` with `queryDeviceStatus`**

Create `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`:

```java
package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceStatusResult;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DeviceMcpToolService {

    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;

    @Tool(description = "Query a device's online status and latest telemetry value per identifier, by device key")
    public DeviceStatusResult queryDeviceStatus(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey) {
        Device device = deviceRepository.findByDeviceKeyAndDeleted(deviceKey, (short) 0).orElse(null);
        if (device == null) {
            return DeviceStatusResult.notFound(deviceKey);
        }
        String productKey = productRepository.findById(device.getProductId())
                .map(Product::getProductKey)
                .orElse(null);
        return new DeviceStatusResult(
                deviceKey,
                true,
                device.getDeviceName(),
                productKey,
                device.getOnlineStatus() == 1,
                device.getLastOnlineTime(),
                device.getLastOfflineTime(),
                deviceDataRepository.findLatestByDeviceKey(deviceKey));
    }
}
```

- [ ] **Step 5: Create `McpToolConfig`**

Create `src/main/java/com/spark/agent/mcp/McpToolConfig.java`:

```java
package com.spark.agent.mcp;

import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class McpToolConfig {

    @Bean
    public ToolCallbackProvider deviceToolCallbacks(DeviceMcpToolService deviceMcpToolService) {
        return MethodToolCallbackProvider.builder()
                .toolObjects(deviceMcpToolService)
                .build();
    }
}
```

- [ ] **Step 6: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 7: Start the app and confirm the MCP server initializes**

Run:
```bash
./gradlew bootRun > /tmp/spark-agent-mcp-boot.log 2>&1 &
echo $! > /tmp/spark-agent-mcp-boot.pid
timeout 90 grep -m1 -q "Started SparkAgentEngineApplication" <(tail -f /tmp/spark-agent-mcp-boot.log) && echo READY
```
Expected: `READY` printed within 90s, and no `ERROR` lines in `/tmp/spark-agent-mcp-boot.log` mentioning `mcp` or `ToolCallbackProvider`.

- [ ] **Step 8: List registered tools via the MCP Inspector CLI**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/list`
Expected: JSON output listing one tool named `queryDeviceStatus` with its description and a `deviceKey` string parameter.

- [ ] **Step 9: Call `queryDeviceStatus` for a known device**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/call --tool-name queryDeviceStatus --tool-arg deviceKey=DK_INJ_001`
Expected: result with `"found":true`, `"deviceName":"injection_01"`, `"productKey":"PK_INJECTION_MA"`, and a non-empty `latestTelemetry` array covering `temperature`/`pressure`/`current`.

- [ ] **Step 10: Call `queryDeviceStatus` for an unknown device**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/call --tool-name queryDeviceStatus --tool-arg deviceKey=DOES_NOT_EXIST`
Expected: result with `"found":false` and `"latestTelemetry":[]`, no tool-call error.

- [ ] **Step 11: Stop the app**

Run: `kill $(cat /tmp/spark-agent-mcp-boot.pid)`

- [ ] **Step 12: Commit**

```bash
git add build.gradle src/main/resources/application.yaml \
  src/main/java/com/spark/agent/dto/DeviceStatusResult.java \
  src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java \
  src/main/java/com/spark/agent/mcp/McpToolConfig.java
git commit -m "feat(mcp): add MCP server and queryDeviceStatus tool"
```

---

### Task 2: `queryDeviceHistory`

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/DeviceDataRepository.java`
- Modify: `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`

**Interfaces:**
- Consumes: `DeviceMcpToolService` from Task 1 (adds a method to the same class; no signature changes to existing methods).
- Produces: `DeviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(String, String, Short, LocalDateTime, Pageable)` — repository method, not consumed elsewhere in this plan.

- [ ] **Step 1: Add the repository method**

Edit `src/main/java/com/spark/agent/repository/DeviceDataRepository.java`, adding after the existing `findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc` method:

```java
    List<DeviceData> findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
            String deviceKey, String identifier, Short deleted, LocalDateTime since,
            org.springframework.data.domain.Pageable pageable);
```

- [ ] **Step 2: Add the `queryDeviceHistory` tool method**

Edit `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`. Add these imports:

```java
import com.spark.agent.entity.DeviceData;
import org.springframework.data.domain.PageRequest;

import java.time.LocalDateTime;
import java.util.List;
```

Add the method inside the class, after `queryDeviceStatus`:

```java
    @Tool(description = "Query historical telemetry values for one identifier of a device over the last N hours, newest first (capped at 500 rows)")
    public List<DeviceData> queryDeviceHistory(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "the telemetry identifier, e.g. temperature, pressure, current") String identifier,
            @ToolParam(description = "how many hours of history to look back from now") int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        return deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                deviceKey, identifier, (short) 0, since, PageRequest.of(0, 500));
    }
```

- [ ] **Step 3: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Start the app**

Run:
```bash
./gradlew bootRun > /tmp/spark-agent-mcp-boot.log 2>&1 &
echo $! > /tmp/spark-agent-mcp-boot.pid
timeout 90 grep -m1 -q "Started SparkAgentEngineApplication" <(tail -f /tmp/spark-agent-mcp-boot.log) && echo READY
```
Expected: `READY` printed, no startup errors (a bad Spring Data method name fails fast here as a `PropertyReferenceException` at context startup).

- [ ] **Step 5: Call `queryDeviceHistory`**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/call --tool-name queryDeviceHistory --tool-arg deviceKey=DK_INJ_001 --tool-arg identifier=temperature --tool-arg hours=1`
Expected: a JSON array of at most 500 `DeviceData`-shaped rows for `DK_INJ_001`/`temperature`, ordered newest `reportTime` first.

- [ ] **Step 6: Stop the app**

Run: `kill $(cat /tmp/spark-agent-mcp-boot.pid)`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/repository/DeviceDataRepository.java \
  src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java
git commit -m "feat(mcp): add queryDeviceHistory tool"
```

---

### Task 3: `queryDeviceAlerts`

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/AlertRecordRepository.java`
- Modify: `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`

**Interfaces:**
- Consumes: `DeviceMcpToolService` from Tasks 1–2 (adds a field and a method).
- Produces: `AlertRecordRepository.findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(String, Short, Pageable)` — not consumed elsewhere in this plan.

- [ ] **Step 1: Add the repository method**

Edit `src/main/java/com/spark/agent/repository/AlertRecordRepository.java`, adding after `findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc`:

```java
    List<AlertRecord> findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
            String deviceKey, Short deleted, org.springframework.data.domain.Pageable pageable);
```

- [ ] **Step 2: Add the `queryDeviceAlerts` tool method**

Edit `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`. Add the field and import:

```java
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.repository.AlertRecordRepository;
```

Add `private final AlertRecordRepository alertRecordRepository;` alongside the existing fields (`deviceRepository`, `productRepository`, `deviceDataRepository`) — `@RequiredArgsConstructor` picks it up automatically.

Add the method, after `queryDeviceHistory`:

```java
    @Tool(description = "Query recent alert records for a device, newest first")
    public List<AlertRecord> queryDeviceAlerts(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "max number of alerts to return; defaults to 20 if omitted or <= 0", required = false) int limit) {
        int effectiveLimit = limit > 0 ? limit : 20;
        return alertRecordRepository.findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                deviceKey, (short) 0, PageRequest.of(0, effectiveLimit));
    }
```

- [ ] **Step 3: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Start the app**

Run:
```bash
./gradlew bootRun > /tmp/spark-agent-mcp-boot.log 2>&1 &
echo $! > /tmp/spark-agent-mcp-boot.pid
timeout 90 grep -m1 -q "Started SparkAgentEngineApplication" <(tail -f /tmp/spark-agent-mcp-boot.log) && echo READY
```
Expected: `READY` printed, no startup errors.

- [ ] **Step 5: Call `queryDeviceAlerts`**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/call --tool-name queryDeviceAlerts --tool-arg deviceKey=DK_INJ_001 --tool-arg limit=0`
Expected: a JSON array of up to 20 `AlertRecord`-shaped rows for `DK_INJ_001` (known to have `temperature`/`pressure` alerts), newest `triggerTime` first — confirms `limit=0` falls back to the default of 20.

- [ ] **Step 6: Stop the app**

Run: `kill $(cat /tmp/spark-agent-mcp-boot.pid)`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/repository/AlertRecordRepository.java \
  src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java
git commit -m "feat(mcp): add queryDeviceAlerts tool"
```

---

### Task 4: `queryDeviceManual`

**Files:**
- Modify: `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`

**Interfaces:**
- Consumes: `DeviceMcpToolService` from Tasks 1–3 (adds a field and a method); pre-existing `RagSearchService.search(String query, String deviceModel, int topK)` and `VectorStoreRepository.SearchResult` (both already used by `RagController`/`DiagnosisAgentService`).

- [ ] **Step 1: Add the `queryDeviceManual` tool method**

Edit `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`. Add the field and import:

```java
import com.spark.agent.repository.VectorStoreRepository;
import com.spark.agent.service.RagSearchService;
```

Add `private final RagSearchService ragSearchService;` alongside the existing fields.

Add the method, after `queryDeviceAlerts`:

```java
    @Tool(description = "Search the device manual/knowledge base for a device model and return relevant excerpts")
    public List<VectorStoreRepository.SearchResult> queryDeviceManual(
            @ToolParam(description = "the device's product model/key, e.g. PK_INJECTION_MA — this is the product_key, not the device's display name; get it from queryDeviceStatus if unknown") String deviceModel,
            @ToolParam(description = "the question or symptom to search the manual for") String question) {
        return ragSearchService.search(question, deviceModel, 5);
    }
```

- [ ] **Step 2: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Start the app**

Run:
```bash
./gradlew bootRun > /tmp/spark-agent-mcp-boot.log 2>&1 &
echo $! > /tmp/spark-agent-mcp-boot.pid
timeout 90 grep -m1 -q "Started SparkAgentEngineApplication" <(tail -f /tmp/spark-agent-mcp-boot.log) && echo READY
```
Expected: `READY` printed, no startup errors.

- [ ] **Step 4: Confirm all 4 tools are registered**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/list`
Expected: 4 tools listed — `queryDeviceStatus`, `queryDeviceHistory`, `queryDeviceAlerts`, `queryDeviceManual` — each with its description and parameters.

- [ ] **Step 5: Call `queryDeviceManual`**

Run: `npx -y @modelcontextprotocol/inspector --cli --transport http --server-url http://localhost:8080/mcp --method tools/call --tool-name queryDeviceManual --tool-arg deviceModel=PK_INJECTION_MA --tool-arg question=机筒温度异常升高`
Expected: a JSON array of `SearchResult`-shaped entries (`id`, `title`, `chunkText`, `docType`, `deviceModel`, `source`, `distance`), at most 5 entries, filtered to `deviceModel=PK_INJECTION_MA` — or an empty array if no matching knowledge rows exist yet (not an error either way).

- [ ] **Step 6: Stop the app**

Run: `kill $(cat /tmp/spark-agent-mcp-boot.pid)`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java
git commit -m "feat(mcp): add queryDeviceManual tool"
```
