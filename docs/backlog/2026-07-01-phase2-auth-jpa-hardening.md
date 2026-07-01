# Phase 2 — Auth / JPA Entity Exposure Hardening (Backlog)

> Closes the two HIGH-severity security findings from the Phase 1 audit that
> were deferred as scope additions.

## Motivation

The Phase 1 audit (2026-07-01) identified two HIGH findings that were classified
as scope additions beyond Phase 1's data-ingestion focus:

- **H19: No authentication/authorization** — no Spring Security, no API Key,
  no CORS. Any network endpoint is fully open.
- **H20: API returns JPA entities directly** — `ApiController` and MCP tools
  return `DeviceData`, `AlertRecord` etc., exposing `deleted`, `tenantId`,
  `updater`, `createTime` etc.

## H19 — Authentication / Authorization

### Scope

- No user management, role hierarchy, or OAuth2 — Phase 2 is an IoT gateway
  and doesn't need multi-user auth.
- Add **API Key authentication** via a custom Spring Security filter:
  - A single configured API key (or a small set) validated on every request.
  - Key stored in `application.yaml` under `app.api-key`.
  - Read-only endpoints (GET) vs. mutation endpoints (POST) can use different
    keys if needed.

### Affected Components

| Component | Change |
|---|---|
| `build.gradle` | Add `org.springframework.boot:spring-boot-starter-security` |
| New: `config/ApiKeyAuthFilter.java` | `OncePerRequestFilter` that reads `X-API-Key` header, validates against config |
| New: `config/SecurityConfig.java` | `@EnableWebSecurity`, register filter, permit actuator/health, protect everything else |
| `application.yaml` | Add `app.api-key` under `app` section (default: empty = auth disabled) |
| `application.yaml` | Add `spring.security.ignored-paths` for health check endpoints |

### Design Decisions

- **Why API Key, not JWT/OAuth2?** The service is an internal data pipeline
  component. API Key is the simplest security primitive that prevents
  unauthorized network access. OAuth2 is over-engineering for an internal
  microservice.
- **Why filter, not method security?** Method-level `@PreAuthorize` is
  unnecessary for a single-permission model. A filter either accepts or rejects
  the request — there's no per-endpoint role gating.
- **Auth disabled by default** when `app.api-key` is empty — preserves
  the current dev/Docker experience. Emit a strong startup log warning if auth
  is disabled in production profile.

### Acceptance Criteria

- [ ] Requests without `X-API-Key` header return `401 Unauthorized`.
- [ ] Requests with wrong `X-API-Key` header return `401 Unauthorized`.
- [ ] Requests with correct `X-API-Key` header pass through normally.
- [ ] Health endpoint (`/actuator/health`) is exempt from auth.
- [ ] When `app.api-key` is empty, auth is disabled (backward compat).
- [ ] Strong startup warning when auth is disabled.
- [ ] Build passes, existing test suite passes.

---

## H20 — JPA Entity Exposure

### Scope

All REST and MCP endpoints currently return JPA entities directly. The `deleted`,
`tenantId`, `creator`, `updater`, `createTime`, `updateTime` fields leak internal
schema details. Fix by introducing response DTOs that expose only intended fields.

### Affected Components

| Component | Current | Target |
|---|---|---|
| `ApiController.latest()` | Returns `R<List<DeviceData>>` | Returns `R<List<DeviceDataResponse>>` |
| `ApiController.history()` | Returns `R<List<DeviceData>>` | Returns `R<List<DeviceDataResponse>>` |
| `ApiController.recentAlerts()` | Returns `R<List<AlertRecord>>` | Returns `R<List<AlertRecordResponse>>` |
| `DeviceMcpToolService` 4 methods | Return JPA entities | Return DTOs |
| New: `dto/DeviceDataResponse.java` | — | Exposes: `deviceKey`, `identifier`, `value`, `valueNum`, `reportTime`, `quality` |
| New: `dto/AlertRecordResponse.java` | — | Exposes: `deviceKey`, `identifier`, `triggerValue`, `level`, `alertContent`, `triggerTime`, `diagnosisStatus`, `rootCause`, `suggestion`, `confidence` |
| New: `dto/DeviceStatusResult.java` | — | Exposes: `deviceKey`, `deviceName`, `onlineStatus`, `lastOnlineTime`, `lastOfflineTime` |

### Design Decisions

- **DTOs per response type, not a generic mapper**: Each response DTO is a
  `public record` with explicit fields. No reflection-based mapping, no
  `ModelMapper` dependency. The mapping is a one-liner in the controller.
- **No `@JsonIgnore` on entities**: Band-aid solution that silently breaks if
  anyone forgets. DTOs are the correct architectural boundary.
- **Add a `DtoMapper` utility class** if the mapping logic is used in more than
  2 places. `DeviceMcpToolService` may share the same DTOs.

### Migration Path

1. Create `DeviceDataResponse` record — copy only the fields listed above.
   Use the same field names as the JSON response.
2. Update `ApiController.latest()` and `history()` to map `DeviceData → DeviceDataResponse`.
3. Create `AlertRecordResponse` record — copy only the fields listed above.
4. Update `ApiController.recentAlerts()` to map `AlertRecord → AlertRecordResponse`.
5. Create `DeviceStatusResult` record (or reuse existing if one exists in MCP).
6. Update `DeviceMcpToolService` 4 methods to return DTOs instead of entities.
7. Verify no JPA entity leaks via `ApiController` or MCP tool responses.
8. Remove `@Getter` from `BaseEntity` or annotate sensitive fields with
   `@JsonIgnore` as a last-resort defense.

### Acceptance Criteria

- [ ] `GET /api/device/{deviceKey}/latest` returns response with only:
      `deviceKey`, `identifier`, `value`, `valueNum`, `reportTime`, `quality`.
- [ ] `GET /api/device/{deviceKey}/history` returns same DTO shape.
- [ ] `GET /api/alert/recent` returns response with only the listed alert fields.
- [ ] MCP tool responses (both JSON and native Java) use DTOs.
- [ ] No JPA entity class leaks into the JSON response or MCP tool return type.
- [ ] Build passes, existing test suite passes.

## Combined Effort Estimate

| Workstream | Files touched | Effort |
|---|---|---|
| API Key auth | 4 new + 2 modified | Small |
| JPA entity DTOs | 3 new DTOs + 4 modified | Medium |

A single developer should complete both in 1-2 days.
