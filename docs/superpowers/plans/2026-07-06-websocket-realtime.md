# WebSocket Real-Time Delivery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a STOMP/WebSocket push layer alongside the existing MQTT→Outbox→Kafka pipeline so the
frontend can replace its 4s poll with real-time telemetry, alert, diagnosis-complete, and
device-status events, without modifying any existing business logic.

**Full spec:** `docs/superpowers/specs/2026-07-06-websocket-realtime-design.md`

**Tech Stack:** Java 25, Spring Boot 4.1.0, `spring-boot-starter-websocket` (new), Spring Kafka,
Jackson 3 (`tools.jackson.databind`), Lombok, JUnit 5 + Mockito.

## Global Constraints

- Same Jackson 3 / `KafkaTemplate<Object,Object>` / snowflake-ID / `ddl-auto: none` gotchas as
  `docs/superpowers/plans/2026-07-02-transactional-outbox.md` — no new entities/tables in this plan,
  so no SQL migration needed.
- Existing test convention: JUnit 5 `@ExtendWith(MockitoExtension.class)`, `@Mock` fields, manual
  `new Xxx(mock1, mock2, ...)` construction in `@BeforeEach` (no `@InjectMocks`).
- If `SimpMessagingTemplate` / `@EnableWebSocketMessageBroker` don't autoconfigure despite
  `spring-boot-starter-websocket` on the classpath, add `org.springframework.boot:spring-boot-websocket`
  explicitly (same module-split gotcha as `spring-boot-kafka`, documented in root `CLAUDE.md`).
- New Kafka consumers use `groupId="ws-bridge"` — a second, independent consumer group on
  `iot.device.data` / `iot.alert.triggered`. This must NOT touch `AlertTriggeredConsumer`
  (groupId=`diagnosis-agent`) or its topic/config references.
- Run tests with `./gradlew test --tests "com.spark.agent.<package>.<ClassName>"`.

---

### Task 1: Dependency + config properties

**Files:**
- Modify: `build.gradle`
- Modify: `src/main/java/com/spark/agent/config/AppProperties.java`
- Modify: `src/main/resources/application.yaml`

- [ ] **Step 1**: Add `implementation 'org.springframework.boot:spring-boot-starter-websocket'` to
  `build.gradle`'s dependencies block, next to `spring-boot-starter-web`.
- [ ] **Step 2**: Add to `AppProperties`: `private int telemetryWsCoalesceIntervalMs = 400;`
  (doc comment: "how often TelemetryWsBridgeConsumer flushes its per-device coalescing buffer").
- [ ] **Step 3**: Add to `application.yaml` under `app:`: `telemetry-ws-coalesce-interval-ms: 400`.
- [ ] **Step 4**: `./gradlew build -x test` — confirm it still compiles with the new dependency.

---

### Task 2: `WsPushService` + `DeviceStatusEvent`

**Files:**
- Create: `src/main/java/com/spark/agent/ws/DeviceStatusEvent.java`
- Create: `src/main/java/com/spark/agent/ws/WsPushService.java`
- Create: `src/test/java/com/spark/agent/ws/WsPushServiceTest.java`

**Interfaces:**
- Produces: `WsPushService.pushTelemetry(String deviceKey, Object snapshot)`,
  `.pushAlert(Object alertRecord)`, `.pushDiagnosis(Long alertId, Object alertRecord)`,
  `.pushDeviceStatus(DeviceStatusEvent event)`. Tasks 3, 4, 5, 6 depend on these exact signatures.
- `DeviceStatusEvent(String deviceKey, boolean online, LocalDateTime changedAt)` — a plain record.

- [ ] **Step 1**: Write `DeviceStatusEvent` as a `record` with the three fields above.
- [ ] **Step 2**: Write the failing test `WsPushServiceTest` — four tests, one per method, each
  verifying `SimpMessagingTemplate.convertAndSend(destination, payload)` is called with the exact
  destination string (`/topic/telemetry/{deviceKey}`, `/topic/alerts`, `/topic/diagnosis/{alertId}`,
  `/topic/devices/status`) and the payload passed through unchanged.
- [ ] **Step 3**: Run test, confirm compile failure (`WsPushService` doesn't exist).
- [ ] **Step 4**: Write `WsPushService` — `@Service @RequiredArgsConstructor`, one
  `SimpMessagingTemplate` field, four one-line methods delegating to `convertAndSend`.
- [ ] **Step 5**: Run test, confirm PASS.
- [ ] **Step 6**: `./gradlew build -x test`.

---

### Task 3: `WebSocketConfig`

**Files:**
- Create: `src/main/java/com/spark/agent/config/WebSocketConfig.java`

No dedicated unit test — matches existing convention for pure Spring wiring config classes
(`KafkaConsumerConfig`, `RedisKeyExpirationConfig`, `McpToolConfig` have none either).

- [ ] **Step 1**: `@Configuration @EnableWebSocketMessageBroker` implementing
  `WebSocketMessageBrokerConfigurer`. `configureMessageBroker`: `registry.enableSimpleBroker("/topic")`.
  `registerStompEndpoints`: `registry.addEndpoint("/ws").setAllowedOriginPatterns("*").withSockJS()`.
- [ ] **Step 2**: `./gradlew build -x test`.

---

### Task 4: `TelemetryWsBridgeConsumer` — per-device coalescing bridge

**Files:**
- Create: `src/main/java/com/spark/agent/kafka/TelemetryWsBridgeConsumer.java`
- Create: `src/test/java/com/spark/agent/kafka/TelemetryWsBridgeConsumerTest.java`

**Interfaces:**
- Consumes: `WsPushService.pushTelemetry` (Task 2).
- New `groupId="ws-bridge"` `@KafkaListener` on `${kafka.topic.device-data:iot.device.data}` —
  independent of `TelemetryService`/`OutboxRelayService`, no changes to either.

- [ ] **Step 1**: Write the failing tests:
  - `onDeviceData_thenFlush_sendsCoalescedSnapshotPerDevice` — feed two `ConsumerRecord`s for the
    same `deviceKey` with different `identifier`/`valueNum` (built by hand-serializing a `DeviceData`
    JSON string, mirroring `MqttSubscriberTest`'s style of constructing raw JSON payloads), call
    `flush()`, verify `wsPushService.pushTelemetry("DK_TEST_001", ...)` called exactly once with a
    snapshot containing both properties merged.
  - `flush_noPendingUpdates_doesNotPush` — call `flush()` with nothing buffered, verify
    `verifyNoInteractions(wsPushService)`.
  - `onDeviceData_invalidJson_doesNotThrowAndSkips` — malformed payload, `assertDoesNotThrow`, then
    `flush()` verifies no push.
  - `flush_multipleDevices_pushesOncePerDeviceToItsOwnTopic` — two different `deviceKey`s buffered,
    verify two separate `pushTelemetry` calls with two distinct device keys (confirms per-device
    topics are preserved, not batched into one call).
- [ ] **Step 2**: Run tests, confirm compile failure.
- [ ] **Step 3**: Write `TelemetryWsBridgeConsumer`:
  - Fields: `ObjectMapper objectMapper`, `WsPushService wsPushService`,
    `Map<String, DeviceSnapshot> pending = new ConcurrentHashMap<>()` (private, not injected).
  - `@KafkaListener(topics = "${kafka.topic.device-data:iot.device.data}", groupId = "ws-bridge")`
    `onDeviceData(ConsumerRecord<String, String> record)`: parse `record.value()` into `DeviceData`
    (catch/log/return on parse failure, same pattern as `AlertTriggeredConsumer`); merge
    `identifier → (valueNum != null ? valueNum : value)` and track the max `reportTime` into
    `pending.computeIfAbsent(deviceKey, k -> new DeviceSnapshot())`.
  - `@Scheduled(fixedDelayString = "${app.telemetry-ws-coalesce-interval-ms:400}") flush()`:
    snapshot `pending.keySet()`, `pending.remove(deviceKey)` each, and
    `wsPushService.pushTelemetry(deviceKey, snapshot.toPayload(deviceKey))` for each non-null removal.
  - Inner `DeviceSnapshot` (properties map + latest `reportTime`) and a `TelemetrySnapshot(String
    deviceKey, Map<String,Object> properties, LocalDateTime reportTime)` record as the pushed payload
    shape.
- [ ] **Step 4**: Run tests, confirm PASS.
- [ ] **Step 5**: `./gradlew build -x test`.

---

### Task 5: `AlertWsBridgeConsumer`

**Files:**
- Create: `src/main/java/com/spark/agent/kafka/AlertWsBridgeConsumer.java`
- Create: `src/test/java/com/spark/agent/kafka/AlertWsBridgeConsumerTest.java`

**Interfaces:**
- Consumes: `WsPushService.pushAlert` (Task 2).
- New `groupId="ws-bridge"` `@KafkaListener` on `${kafka.topic.alert-triggered:iot.alert.triggered}` —
  same topic as `AlertTriggeredConsumer`, different group, so it does not affect that consumer's
  offsets, retries, or the diagnosis pipeline at all.

- [ ] **Step 1**: Write failing tests mirroring `AlertTriggeredConsumer`'s (nonexistent, so write
  fresh) shape:
  - `onAlertTriggered_validPayload_pushesAlertRecord` — build a `ConsumerRecord` with a JSON
    `AlertRecord` body, verify `wsPushService.pushAlert(argThat(record with matching id))`.
  - `onAlertTriggered_invalidJson_doesNotThrowAndSkipsPush` — malformed payload,
    `assertDoesNotThrow`, `verifyNoInteractions(wsPushService)`.
- [ ] **Step 2**: Run tests, confirm compile failure.
- [ ] **Step 3**: Write `AlertWsBridgeConsumer` — same shape as `AlertTriggeredConsumer` (parse
  `AlertRecord`, catch/log/return on failure) but call `wsPushService.pushAlert(alert)` instead of
  `diagnosisAgentService.diagnose(alertId)`.
- [ ] **Step 4**: Run tests, confirm PASS.
- [ ] **Step 5**: `./gradlew build -x test`.

---

### Task 6: Hook `DiagnosisAgentService.writeBack()`

**Files:**
- Modify: `src/main/java/com/spark/agent/service/DiagnosisAgentService.java`
- Modify: `src/test/java/com/spark/agent/service/DiagnosisAgentServiceTest.java`

**Interfaces:**
- Consumes: `WsPushService.pushDiagnosis(Long, Object)` (Task 2).
- No change to `diagnose(Long alertId)`'s signature or return value.

- [ ] **Step 1**: Update `DiagnosisAgentServiceTest`: add `@Mock private WsPushService wsPushService;`
  and pass it into the `new DiagnosisAgentService(...)` constructor call in `setUp()`. Add
  `verify(wsPushService).pushDiagnosis(eq(100L), any(AlertRecord.class));` to
  `diagnose_highConfidence_setsDiagnosisStatus2` and `diagnose_lowConfidence_writesHumanReviewStatus`.
- [ ] **Step 2**: Run tests, confirm compile failure (constructor arity mismatch).
- [ ] **Step 3**: Add `private final WsPushService wsPushService;` field to `DiagnosisAgentService`;
  in `writeBack(AlertRecord record, DiagnosisResult result)`, add
  `wsPushService.pushDiagnosis(record.getId(), record);` immediately after
  `alertRecordRepository.save(record);`.
- [ ] **Step 4**: Run tests, confirm PASS.
- [ ] **Step 5**: `./gradlew build -x test`.

---

### Task 7: Hook `DeviceHeartbeatService` transitions

**Files:**
- Modify: `src/main/java/com/spark/agent/service/DeviceHeartbeatService.java`
- Modify: `src/test/java/com/spark/agent/service/DeviceHeartbeatServiceTest.java`

**Interfaces:**
- Consumes: `WsPushService.pushDeviceStatus(DeviceStatusEvent)` (Task 2).
- No change to `heartbeat`/`markOfflineNow`/`markAllOffline`/`onMessage` signatures.

- [ ] **Step 1**: Update `DeviceHeartbeatServiceTest`: add `@Mock private WsPushService
  wsPushService;`, pass into constructor. Add new tests:
  - `heartbeat_deviceComesOnline_pushesOnlineStatus`
  - `markOfflineNow_deviceExists_pushesOfflineStatus` (extend existing test)
  - `markAllOffline_multipleOnlineDevices_pushesOfflineStatusForEach` (extend existing test)
  - `onMessage_keyExpiredAndStillAbsent_pushesOfflineStatus` (new — `onMessage` currently untested)
- [ ] **Step 2**: Run tests, confirm compile failure.
- [ ] **Step 3**: Add `private final WsPushService wsPushService;` field; add one
  `wsPushService.pushDeviceStatus(new DeviceStatusEvent(deviceKey, true/false, LocalDateTime.now()))`
  call at each of the 4 transition points (after `markOnline`/`markOffline` DB writes, inside the
  same branch that logs the transition).
- [ ] **Step 4**: Run tests, confirm PASS.
- [ ] **Step 5**: `./gradlew build -x test`.

---

### Task 8: Full verification

- [ ] **Step 1**: `./gradlew build` — full compile + test suite, confirm no regressions in any
  existing test class.
- [ ] **Step 2**: Manual smoke test (requires docker stack + `./gradlew bootRun`): connect a STOMP
  client (e.g. browser devtools with a SockJS/stomp.js snippet, or `wscat` against `/ws`) and
  subscribe to `/topic/alerts`; trigger an alert via the simulator or a manual MQTT publish; confirm
  a message arrives within ~2s (outbox relay interval) without needing to poll.

## Post-Plan Note

This plan is backend-only (Phase 3 of the broader real-time initiative). Phase 4 (frontend
`src/lib/ws/` client, `useDeviceTelemetryWS`/`useAlertWS`/`useDiagnosisWS` hooks replacing
`useTelemetrySync`'s poll) is a separate plan against `spark-agent-screen`, gated on this one being
merged and manually verified end-to-end.
