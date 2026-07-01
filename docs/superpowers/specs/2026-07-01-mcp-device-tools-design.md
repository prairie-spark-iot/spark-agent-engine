# MCP Device Tools (Phase 4.3.1) — Design

## Purpose

Expose the device query capabilities already backing the REST API (`ApiController`, `RagController`) as MCP tools, so external AI agents (and, in 4.3.2, this project's own diagnosis agent) can call them via the Model Context Protocol instead of hardcoded REST calls.

Per CLAUDE.md phase 4: "expose device query and alert management as MCP tools for external AI agents." This phase covers the 4 read-only query tools only. No control/action tools are built now — the human-confirmation requirement for control-type tools is a future-phase constraint, not something implemented here.

## Transport

`spring-ai-starter-mcp-server-webmvc`, added alongside the existing `spring-ai-bom:2.0.0` platform. The MCP server runs inside the existing Spring Boot web app on `:8080` — no second port or process. This matches "external AI agents" reaching the server over the network, and keeps deployment identical to today.

`application.yaml` gets a minimal `spring.ai.mcp.server` block (`name`, `version`, `type: SYNC`). The exact resolved endpoint path is a starter default; it will be confirmed empirically at implementation/verification time (documented in the plan's verification step) rather than assumed here.

## Components

| File | Change |
|---|---|
| `build.gradle` | Add `implementation 'org.springframework.ai:spring-ai-starter-mcp-server-webmvc'` |
| `application.yaml` | Add `spring.ai.mcp.server: { name, version, type: SYNC }` |
| `mcp/DeviceMcpToolService.java` | New `@Service`. Four `@Tool`-annotated methods (see below), each delegating to existing repositories/services — no new business logic, just a thin MCP-facing wrapper. |
| `mcp/McpToolConfig.java` | New `@Configuration`. One `@Bean ToolCallbackProvider` built via `MethodToolCallbackProvider.builder().toolObjects(deviceMcpToolService).build()` — Spring AI's MCP server auto-configuration discovers this bean and registers its tools automatically. |
| `dto/DeviceStatusResult.java` | New record (see below) — combines `Device` + latest telemetry, since no existing endpoint returns this combined shape. |
| `repository/DeviceDataRepository.java` | New derived-query method for the time-windowed history lookup (see below). |
| `repository/AlertRecordRepository.java` | New derived-query method for the device-scoped alert list with a caller-supplied limit (see below). |

## The 4 Tools

All four are read-only and reuse the same repositories/services the REST controllers already use — this is a second, thin entry point over existing logic, not a parallel implementation.

### `queryDeviceStatus(deviceKey: String) -> DeviceStatusResult`

```java
public record DeviceStatusResult(
    String deviceKey,
    boolean found,
    String deviceName,
    String productKey,           // device model — for chaining into queryDeviceManual
    boolean online,
    LocalDateTime lastOnlineTime,
    LocalDateTime lastOfflineTime,
    List<DeviceData> latestTelemetry
) {}
```

- Looks up `Device` via `DeviceRepository.findByDeviceKeyAndDeleted(deviceKey, 0)`.
- If not found: `found=false`, all other fields `null`/empty — no exception thrown, so the LLM gets a clean structured "not found" answer instead of a tool-call error.
- If found: resolves `productKey` via `ProductRepository.findById(device.getProductId())` (same product_key convention `DiagnosisAgentService` already uses for RAG lookups — device model, not device name), and fetches `latestTelemetry` via the existing `DeviceDataRepository.findLatestByDeviceKey(deviceKey)`.
- `productKey` is included specifically so an agent can chain: call `queryDeviceStatus` to learn the device's model, then pass that model into `queryDeviceManual`.

### `queryDeviceHistory(deviceKey: String, identifier: String, hours: int) -> List<DeviceData>`

- New repository method:
  ```java
  List<DeviceData> findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
          String deviceKey, String identifier, Short deleted, LocalDateTime since, Pageable pageable);
  ```
- Tool computes `since = LocalDateTime.now().minusHours(hours)` and calls with `PageRequest.of(0, 500)` — capped at 500 rows regardless of how large `hours` is, newest first.
- Returns `List<DeviceData>` directly — same entity shape the existing `/api/device/{deviceKey}/history` REST endpoint returns.

### `queryDeviceAlerts(deviceKey: String, limit: int) -> List<AlertRecord>`

- New repository method:
  ```java
  List<AlertRecord> findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(String deviceKey, Short deleted, Pageable pageable);
  ```
- Tool defaults `limit` to 20 when `<= 0` (mirrors `/api/alert/recent`'s convention).
- Returns `List<AlertRecord>` directly — includes existing diagnosis fields (`rootCause`, `suggestion`, `confidence`, etc.) since they're already on the entity.

### `queryDeviceManual(deviceModel: String, question: String) -> List<VectorStoreRepository.SearchResult>`

- Delegates directly to `RagSearchService.search(question, deviceModel, 5)` — same call `RagController`'s `/api/rag/search` makes.
- `topK` is fixed at 5 internally, not exposed as a tool parameter (matches `DiagnosisAgentService`'s existing use of `ragSearchService.search(..., 3)` in spirit — a small fixed top-K, not caller-controlled).
- `@ToolParam` description on `deviceModel` clarifies it means the product_key (e.g. `"PK_INJECTION_MA"`), not the device's display name — a distinction the codebase already had to fix once (see the `product_key` fix commit).

## Data Flow

```
External MCP client (or, in 4.3.2, this app's own diagnosis agent)
  │
  ▼
Spring AI MCP Server (webmvc, embedded in :8080)
  │  dispatches tool call by name
  ▼
DeviceMcpToolService.<tool>(...)
  ├─► queryDeviceStatus    → DeviceRepository + ProductRepository + DeviceDataRepository.findLatestByDeviceKey
  ├─► queryDeviceHistory   → DeviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc
  ├─► queryDeviceAlerts    → AlertRecordRepository.findByDeviceKeyAndDeletedOrderByTriggerTimeDesc
  └─► queryDeviceManual    → RagSearchService.search(question, deviceModel, 5)
```

## Error Handling

- `queryDeviceStatus`: device-not-found is a normal structured result (`found=false`), not an exception — an MCP tool-call error is a worse experience for an LLM caller than a clean negative result.
- The other three tools: an unknown `deviceKey`/`identifier` simply yields an empty list — no special-casing, matching how the existing REST endpoints already behave for unknown keys.
- No new validation beyond what the repositories/services already do — these are read-only queries; there's nothing to protect against beyond the row caps already specified (500 for history, default-20 for alerts, fixed-5 for manual search).

## Out of Scope

- No control/action tools (e.g. acknowledge alert, restart device) — read-only only, per requirement.
- No changes to existing REST controllers (`ApiController`, `RagController`) — they remain the REST entry points; the MCP tools are additive.
- No changes to `DiagnosisAgentService` — its tool-calling rewrite is 4.3.2, a separate design.
- No auth/access-control on the MCP endpoint — matches the current REST API's lack of auth; out of scope for this phase.

## Testing

This repo currently has no unit-test scaffolding for services (only the Spring context-load test exists), so verification is manual:
- Start the app, confirm the MCP server initializes without error and locate the actual resolved endpoint path from startup logs.
- Drive each of the 4 tools end-to-end (via an MCP inspector client or equivalent) against real data already in Postgres, including at least one not-found case (`queryDeviceStatus` with an unknown `deviceKey`) and one empty-result case.
