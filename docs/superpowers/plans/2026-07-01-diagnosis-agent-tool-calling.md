# Diagnosis Agent Tool-Calling Rewrite (Phase 4.3.2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `DiagnosisAgentService`'s hardcoded gather-then-reflect diagnosis flow with a true tool-calling agent that decides for itself, per alert, which of the 4.3.1 MCP tools to call before producing a diagnosis.

**Architecture:** `DiagnosisAgentService.diagnose()` drops all manual context-gathering (device lookup, telemetry fetch, alert history, manual search) and the reflection retry pass. It sends the LLM only the raw alert fields plus the existing `deviceToolCallbacks` `ToolCallbackProvider` bean (already registered by 4.3.1's `McpToolConfig` for external MCP clients); Spring AI's tool-calling loop lets the model call `queryDeviceStatus` / `queryDeviceHistory` / `queryDeviceAlerts` / `queryDeviceManual` zero or more times before returning its final `DiagnosisResult`. The `diagnosisReflection*` config becomes dead and is deleted. `DeviceMcpToolService`'s 4 tool methods get one debug log line each so the tool-call sequence is observable during manual verification (used both by external MCP clients and by the diagnosis agent itself, since it's the same tool service).

**Tech Stack:** Spring AI 2.0.0 `ChatClient` (`org.springframework.ai.chat.client.ChatClient`), `ToolCallbackProvider` (`org.springframework.ai.tool.ToolCallbackProvider`) — confirmed API: `ChatClient.ChatClientRequestSpec.toolCallbacks(ToolCallbackProvider... toolCallbackProviders)` exists in `spring-ai-client-chat:2.0.0`.

## Global Constraints

- Java 25 + Spring Boot 4.1.0 + Gradle 9.5 — use `./gradlew`, not system gradle.
- Jackson 3.x package rename applies: `tools.jackson.databind.ObjectMapper`, not `com.fasterxml.jackson.databind` (annotations stay at `com.fasterxml.jackson.annotation`).
- `spring.jpa.hibernate.ddl-auto: none` — no schema changes in this plan (none needed).
- No unit-test scaffolding exists for services in this project (only the Spring context-load test) — verification is manual/integration per each task, matching the pattern already used for 4.3.1.
- `deviceToolCallbacks` bean (in `mcp/McpToolConfig.java`) is unchanged and reused as-is — do not modify `McpToolConfig.java` or `DeviceMcpToolService`'s tool signatures/behavior in this plan, only add logging (Task 1).
- `diagnosisConfidenceThreshold` (in `AppProperties`) is unchanged — still gates `writeBack`'s auto-diagnosed vs. human-review-required split.

---

### Task 1: Add tool-call debug logging to `DeviceMcpToolService`

**Files:**
- Modify: `src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`

**Interfaces:**
- Consumes: nothing new — existing `@Tool` method signatures (`queryDeviceStatus(String deviceKey)`, `queryDeviceHistory(String deviceKey, String identifier, int hours)`, `queryDeviceAlerts(String deviceKey, int limit)`, `queryDeviceManual(String deviceModel, String question)`) are unchanged.
- Produces: nothing new — this task only adds logging, no new methods, fields, or beans. Task 2 does not depend on this task's changes (it depends on the pre-existing `deviceToolCallbacks` bean from 4.3.1, not on this logging).

- [ ] **Step 1: Add `@Slf4j` and one debug log line per tool method**

Current file (`src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java`) is:

```java
package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceStatusResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.repository.VectorStoreRepository;
import com.spark.agent.service.RagSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DeviceMcpToolService {

    private final AlertRecordRepository alertRecordRepository;
    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final RagSearchService ragSearchService;

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

    @Tool(description = "Query historical telemetry values for one identifier of a device over the last N hours, newest first (capped at 500 rows)")
    public List<DeviceData> queryDeviceHistory(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "the telemetry identifier, e.g. temperature, pressure, current") String identifier,
            @ToolParam(description = "how many hours of history to look back from now") int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        return deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                deviceKey, identifier, (short) 0, since, PageRequest.of(0, 500));
    }

    @Tool(description = "Query recent alert records for a device, newest first")
    public List<AlertRecord> queryDeviceAlerts(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "max number of alerts to return; defaults to 20 if omitted or <= 0, capped at 500", required = false) int limit) {
        int effectiveLimit = Math.min(limit > 0 ? limit : 20, 500);
        return alertRecordRepository.findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                deviceKey, (short) 0, PageRequest.of(0, effectiveLimit));
    }

    @Tool(description = "Search the device manual/knowledge base for a device model and return relevant excerpts")
    public List<VectorStoreRepository.SearchResult> queryDeviceManual(
            @ToolParam(description = "the device's product model/key, e.g. PK_INJECTION_MA — this is the product_key, not the device's display name; get it from queryDeviceStatus if unknown") String deviceModel,
            @ToolParam(description = "the question or symptom to search the manual for") String question) {
        return ragSearchService.search(question, deviceModel, 5);
    }
}
```

Replace the whole file with:

```java
package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceStatusResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.repository.VectorStoreRepository;
import com.spark.agent.service.RagSearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceMcpToolService {

    private final AlertRecordRepository alertRecordRepository;
    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final RagSearchService ragSearchService;

    @Tool(description = "Query a device's online status and latest telemetry value per identifier, by device key")
    public DeviceStatusResult queryDeviceStatus(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey) {
        log.debug("[MCP Tool] queryDeviceStatus deviceKey={}", deviceKey);
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

    @Tool(description = "Query historical telemetry values for one identifier of a device over the last N hours, newest first (capped at 500 rows)")
    public List<DeviceData> queryDeviceHistory(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "the telemetry identifier, e.g. temperature, pressure, current") String identifier,
            @ToolParam(description = "how many hours of history to look back from now") int hours) {
        log.debug("[MCP Tool] queryDeviceHistory deviceKey={} identifier={} hours={}", deviceKey, identifier, hours);
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        return deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                deviceKey, identifier, (short) 0, since, PageRequest.of(0, 500));
    }

    @Tool(description = "Query recent alert records for a device, newest first")
    public List<AlertRecord> queryDeviceAlerts(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "max number of alerts to return; defaults to 20 if omitted or <= 0, capped at 500", required = false) int limit) {
        log.debug("[MCP Tool] queryDeviceAlerts deviceKey={} limit={}", deviceKey, limit);
        int effectiveLimit = Math.min(limit > 0 ? limit : 20, 500);
        return alertRecordRepository.findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                deviceKey, (short) 0, PageRequest.of(0, effectiveLimit));
    }

    @Tool(description = "Search the device manual/knowledge base for a device model and return relevant excerpts")
    public List<VectorStoreRepository.SearchResult> queryDeviceManual(
            @ToolParam(description = "the device's product model/key, e.g. PK_INJECTION_MA — this is the product_key, not the device's display name; get it from queryDeviceStatus if unknown") String deviceModel,
            @ToolParam(description = "the question or symptom to search the manual for") String question) {
        log.debug("[MCP Tool] queryDeviceManual deviceModel={} question={}", deviceModel, question);
        return ragSearchService.search(question, deviceModel, 5);
    }
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Manually verify the debug logs appear**

Start the app with the MCP tool package bumped to DEBUG (a one-off override, not a permanent `application.yaml` change — keep default log levels as they are):

```bash
./gradlew bootRun --args='--logging.level.com.spark.agent.mcp=DEBUG'
```

In a second terminal, call the tool over the MCP streamable-http endpoint (same handshake 4.3.1 verified — `initialize` → `notifications/initialized` → `tools/call`) against a real device key already in Postgres, e.g.:

```bash
curl -s -X POST http://localhost:8080/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"verify","version":"1.0"}}}'
# note the Mcp-Session-Id response header, then:
curl -s -X POST http://localhost:8080/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H 'Mcp-Session-Id: <session-id-from-above>' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'
curl -s -X POST http://localhost:8080/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H 'Mcp-Session-Id: <session-id-from-above>' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"queryDeviceStatus","arguments":{"deviceKey":"DK_INJ_001"}}}'
```

Expected: the app log shows a line matching `[MCP Tool] queryDeviceStatus deviceKey=DK_INJ_001` at DEBUG level, and the `tools/call` response is unchanged from 4.3.1's verified shape (`found:true`, etc.). Stop the app (`Ctrl+C`) once confirmed.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/spark/agent/mcp/DeviceMcpToolService.java
git commit -m "feat(mcp): add debug logging to device tool methods for tool-call visibility"
```

---

### Task 2: Rewrite `DiagnosisAgentService` to use tool-calling; remove reflection config

**Files:**
- Modify: `src/main/java/com/spark/agent/service/DiagnosisAgentService.java`
- Modify: `src/main/java/com/spark/agent/config/AppProperties.java`
- Modify: `src/main/resources/application.yaml`

**Interfaces:**
- Consumes: `ToolCallbackProvider deviceToolCallbacks` bean, already defined in `src/main/java/com/spark/agent/mcp/McpToolConfig.java` (unchanged, from 4.3.1):
  ```java
  @Bean
  public ToolCallbackProvider deviceToolCallbacks(DeviceMcpToolService deviceMcpToolService) {
      return MethodToolCallbackProvider.builder()
              .toolObjects(new Object[]{deviceMcpToolService})
              .build();
  }
  ```
  Also consumes `ChatClient.ChatClientRequestSpec.toolCallbacks(ToolCallbackProvider... toolCallbackProviders)`, confirmed present in `spring-ai-client-chat:2.0.0`'s `ChatClient` interface.
- Produces: `DiagnosisAgentService.diagnose(String alertMessage)` returns `DiagnosisResult` — signature unchanged; `src/main/java/com/spark/agent/kafka/AlertTriggeredConsumer.java` calls this method and requires no changes.

- [ ] **Step 1: Remove the two dead reflection properties from `AppProperties`**

Current file (`src/main/java/com/spark/agent/config/AppProperties.java`) is:

```java
package com.spark.agent.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private int alertDebounceMinutes = 5;
    private int deviceHeartbeatTtlSeconds = 30;
    private String deviceHeartbeatKeyPrefix = "device:online:";

    /** confidence (0-100) at/above which a diagnosis is accepted without human review */
    private int diagnosisConfidenceThreshold = 80;
    /** confidence (0-100) below which the reflection retry kicks in */
    private int diagnosisReflectionConfidenceThreshold = 40;
    /** telemetry lookback window used only during the reflection retry */
    private int diagnosisReflectionTelemetryWindowMinutes = 120;
}
```

Replace with:

```java
package com.spark.agent.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private int alertDebounceMinutes = 5;
    private int deviceHeartbeatTtlSeconds = 30;
    private String deviceHeartbeatKeyPrefix = "device:online:";

    /** confidence (0-100) at/above which a diagnosis is accepted without human review */
    private int diagnosisConfidenceThreshold = 80;
}
```

- [ ] **Step 2: Remove the two dead reflection keys from `application.yaml`**

Find these lines (currently around line 144-150 of `src/main/resources/application.yaml`):

```yaml
  # diagnosis confidence gate — >= this, diagnosis_status=2 (auto); below, human_review_required
  diagnosis-confidence-threshold: 80
  # diagnosis reflection retry — below this confidence (or empty RAG results), retry once with more context
  diagnosis-reflection-confidence-threshold: 40
  # widened telemetry lookback (minutes) used only on the reflection retry pass
  diagnosis-reflection-telemetry-window-minutes: 120
```

Replace with:

```yaml
  # diagnosis confidence gate — >= this, diagnosis_status=2 (auto); below, human_review_required
  diagnosis-confidence-threshold: 80
```

- [ ] **Step 3: Rewrite `DiagnosisAgentService`**

Current file (`src/main/java/com/spark/agent/service/DiagnosisAgentService.java`) is:

```java
package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.repository.VectorStoreRepository;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisAgentService {

    private static final String SYSTEM_PROMPT = """
            You are an expert industrial IoT diagnosis assistant for factory equipment.
            Given an alert, the device's recent telemetry, its alert history, and excerpts
            from equipment manuals, determine the most likely root cause and a concrete,
            actionable remediation. Be specific and concise.
            """;

    private static final String REFLECTION_INSTRUCTION =
            "\n## Reflection\nYour previous analysis had low confidence or lacked manual references. " +
            "Re-analyze with deeper causal relationships using the widened telemetry window above.\n";

    /** diagnosis_status values written back to aiot_alert_record */
    private static final short STATUS_HUMAN_REVIEW_REQUIRED = 1;
    private static final short STATUS_DIAGNOSED = 2;

    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final RagSearchService ragSearchService;
    private final ChatClient.Builder chatClientBuilder;
    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

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

        Device device = deviceRepository.findByDeviceKeyAndDeleted(alert.getDeviceKey(), (short) 0)
                .orElse(null);
        String deviceModel = device != null
                ? productRepository.findById(device.getProductId()).map(Product::getProductKey).orElse(null)
                : null;

        List<AlertRecord> pastAlerts = alertRecordRepository
                .findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(alert.getDeviceKey(), (short) 0);
        String alertHistory = pastAlerts.stream()
                .map(a -> "[%s] %s (level=%d) @ %s".formatted(a.getIdentifier(), a.getAlertContent(), a.getLevel(), a.getTriggerTime()))
                .collect(Collectors.joining("\n"));

        List<VectorStoreRepository.SearchResult> manuals =
                ragSearchService.search(alert.getAlertContent(), deviceModel, 3);
        String manualExcerpts = formatManuals(manuals);

        List<DeviceData> latestTelemetry = deviceDataRepository.findLatestByDeviceKey(alert.getDeviceKey());
        String telemetryTrend = formatTelemetry(latestTelemetry, "latest per identifier");

        String initialPrompt = buildUserPrompt(alert, device, telemetryTrend, alertHistory, manualExcerpts, false);
        DiagnosisResult result = runInference(initialPrompt);

        if (needsReflection(result, manuals)) {
            log.warn("[Diagnosis][Reflection] alert={} device={} confidence={} manualsFound={} — " +
                            "retrying once with a widened telemetry window and a deeper-analysis prompt",
                    alert.getId(), alert.getDeviceKey(), result.confidence(), manuals.size());

            LocalDateTime since = LocalDateTime.now().minusMinutes(appProperties.getDiagnosisReflectionTelemetryWindowMinutes());
            List<DeviceData> widenedTelemetry = deviceDataRepository
                    .findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                            alert.getDeviceKey(), (short) 0, since, PageRequest.of(0, 100));
            String widenedTrend = formatTelemetry(widenedTelemetry,
                    "last %d minutes".formatted(appProperties.getDiagnosisReflectionTelemetryWindowMinutes()));

            String reflectionPrompt = buildUserPrompt(alert, device, widenedTrend, alertHistory, manualExcerpts, true);
            DiagnosisResult reflected = runInference(reflectionPrompt);

            result = new DiagnosisResult(
                    reflected.rootCause(), reflected.suggestion(), reflected.confidence(),
                    """
                    === Initial Analysis (confidence=%d) ===
                    %s

                    === Reflection Retry (confidence=%d) ===
                    %s
                    """.formatted(result.confidence(), result.diagnosisDetail(),
                            reflected.confidence(), reflected.diagnosisDetail()));
        }

        writeBack(alert, result);
        return result;
    }

    private DiagnosisResult runInference(String userPrompt) {
        return chatClient.prompt()
                .system(SYSTEM_PROMPT)
                .user(userPrompt)
                .call()
                .entity(DiagnosisResult.class);
    }

    private boolean needsReflection(DiagnosisResult result, List<VectorStoreRepository.SearchResult> manuals) {
        return result.confidence() < appProperties.getDiagnosisReflectionConfidenceThreshold() || manuals.isEmpty();
    }

    private void writeBack(AlertRecord record, DiagnosisResult result) {
        boolean autoDiagnosed = result.confidence() >= appProperties.getDiagnosisConfidenceThreshold();
        record.setRootCause(result.rootCause());
        record.setSuggestion(result.suggestion());
        record.setConfidence(BigDecimal.valueOf(result.confidence()));
        record.setDiagnosisDetail(result.diagnosisDetail());
        record.setDiagnosisStatus(autoDiagnosed ? STATUS_DIAGNOSED : STATUS_HUMAN_REVIEW_REQUIRED);
        record.setDiagnosisTime(LocalDateTime.now());
        record.setDeleted((short) 0);
        alertRecordRepository.save(record);

        log.info("[Diagnosis] alert={} status={} confidence={}", record.getId(),
                autoDiagnosed ? "AUTO_DIAGNOSED" : "HUMAN_REVIEW_REQUIRED", result.confidence());
    }

    private String formatTelemetry(List<DeviceData> telemetry, String windowLabel) {
        String trend = telemetry.stream()
                .map(d -> "%s=%s @ %s".formatted(d.getIdentifier(), d.getValue(), d.getReportTime()))
                .collect(Collectors.joining("\n"));
        return trend.isBlank() ? "no data (%s)".formatted(windowLabel) : trend;
    }

    private String formatManuals(List<VectorStoreRepository.SearchResult> manuals) {
        String excerpts = manuals.stream()
                .map(m -> "[%s] %s".formatted(m.title(), m.chunkText()))
                .collect(Collectors.joining("\n---\n"));
        return excerpts.isBlank() ? "none found" : excerpts;
    }

    private String buildUserPrompt(AlertRecord alert, Device device, String telemetryTrend,
                                    String alertHistory, String manualExcerpts, boolean reflection) {
        return """
                ## Alert
                Device: %s
                Identifier: %s
                Trigger value: %s
                Level: %d
                Content: %s
                Trigger time: %s

                ## Device Info
                %s

                ## Recent Telemetry (%s)
                %s

                ## Recent Alert History (same device)
                %s

                ## Relevant Manual Excerpts
                %s
                %s""".formatted(
                alert.getDeviceKey(), alert.getIdentifier(), alert.getTriggerValue(), alert.getLevel(),
                alert.getAlertContent(), alert.getTriggerTime(),
                device != null ? "%s (key=%s)".formatted(device.getDeviceName(), device.getDeviceKey()) : "unknown",
                reflection ? "widened window" : "latest per identifier",
                telemetryTrend,
                alertHistory.isBlank() ? "none" : alertHistory,
                manualExcerpts,
                reflection ? REFLECTION_INSTRUCTION : "");
    }
}
```

Replace the whole file with:

```java
package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.repository.AlertRecordRepository;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisAgentService {

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

    /** diagnosis_status values written back to aiot_alert_record */
    private static final short STATUS_HUMAN_REVIEW_REQUIRED = 1;
    private static final short STATUS_DIAGNOSED = 2;

    private final AlertRecordRepository alertRecordRepository;
    private final ChatClient.Builder chatClientBuilder;
    private final ToolCallbackProvider deviceToolCallbacks;
    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;

    private ChatClient chatClient;

    @PostConstruct
    void init() {
        this.chatClient = chatClientBuilder.build();
    }

    @Transactional
    public DiagnosisResult diagnose(String alertMessage) {
        AlertRecord kafkaAlert;
        try {
            kafkaAlert = objectMapper.readValue(alertMessage, AlertRecord.class);
        } catch (Exception e) {
            log.error("[Diagnosis] Failed to parse alert message: {}", e.getMessage());
            return null;
        }

        // The Kafka payload is a detached, possibly-incomplete snapshot (e.g. deleted defaults to 0
        // via Jackson but any field the producer omitted comes back null). Re-fetch the managed row
        // from Postgres and do all reads/writes against that instead of the wire object.
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

    private void writeBack(AlertRecord record, DiagnosisResult result) {
        boolean autoDiagnosed = result.confidence() >= appProperties.getDiagnosisConfidenceThreshold();
        record.setRootCause(result.rootCause());
        record.setSuggestion(result.suggestion());
        record.setConfidence(BigDecimal.valueOf(result.confidence()));
        record.setDiagnosisDetail(result.diagnosisDetail());
        record.setDiagnosisStatus(autoDiagnosed ? STATUS_DIAGNOSED : STATUS_HUMAN_REVIEW_REQUIRED);
        record.setDiagnosisTime(LocalDateTime.now());
        record.setDeleted((short) 0); // a diagnosis writeback must never leave the record logically deleted
        alertRecordRepository.save(record);

        log.info("[Diagnosis] alert={} status={} confidence={}", record.getId(),
                autoDiagnosed ? "AUTO_DIAGNOSED" : "HUMAN_REVIEW_REQUIRED", result.confidence());
    }
}
```

- [ ] **Step 4: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`. If it fails with "no bean of type ToolCallbackProvider" or similar, confirm `mcp/McpToolConfig.java` still defines the `deviceToolCallbacks` bean unmodified (it should — this task does not touch that file).

- [ ] **Step 5: Manually verify end-to-end via the live alert pipeline**

Start the app with the MCP tool package and the diagnosis service at DEBUG so both the agent's tool calls and its final result are visible:

```bash
./gradlew bootRun --args='--logging.level.com.spark.agent.mcp=DEBUG --logging.level.com.spark.agent.service.DiagnosisAgentService=DEBUG'
```

Let the existing MQTT→Kafka pipeline trigger a real alert (or publish one manually to the `/sys/+/+/thing/event/property/post` MQTT topic per the format in `CLAUDE.md`, with a property value that crosses an active `aiot_alert_rule` threshold).

Tail the app log and confirm:
- One or more `[MCP Tool] query...` DEBUG lines appear before the `[Diagnosis] alert=... status=...` INFO line — this confirms the agent actually invoked tools rather than answering blind.
- The `[Diagnosis] alert=... status=... confidence=...` line appears exactly once per alert (no reflection retry line — that log statement no longer exists).

Then query Postgres directly to confirm the write-back:

```bash
docker exec -it $(docker ps --filter "name=postgres" -q) psql -U root -d spark_ai -c \
  "SELECT id, device_key, identifier, root_cause, suggestion, confidence, diagnosis_status FROM aiot_alert_record ORDER BY diagnosis_time DESC LIMIT 1;"
```

Expected: the most recent row has non-null `root_cause` and `suggestion`, a `confidence` between 0 and 100, and `diagnosis_status` of `1` or `2`. Stop the app (`Ctrl+C`) once confirmed.

Note: this is the first real run of qwen2.5:7b's tool-calling in this codebase — its reliability at 7B scale is unproven. If the model fails to call tools sensibly or the `.entity(DiagnosisResult.class)` parse throws, that is a finding to report, not a plan defect to silently work around — surface it rather than adding retry/fallback logic not in this plan.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/spark/agent/service/DiagnosisAgentService.java \
        src/main/java/com/spark/agent/config/AppProperties.java \
        src/main/resources/application.yaml
git commit -m "feat(diagnosis): rewrite agent to use tool-calling, remove reflection retry"
```
