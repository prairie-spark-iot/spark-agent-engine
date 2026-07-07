# WebSocket Real-Time Delivery Layer — Design

## Goal

Replace the frontend's single 4-second poll (`useTelemetrySync`, `refetchInterval: 4000`) with
server-push for the four flows that are currently only as fresh as the next poll tick: device
telemetry, new/changed alerts, diagnosis completion, and device online/offline transitions.

## Non-goals

- No change to any existing business logic, Kafka topic, MQTT flow, or REST endpoint.
- No removal of Kafka or MQTT — this is an additional delivery layer sitting downstream of the
  existing transactional-outbox pipeline (see `2026-07-02-transactional-outbox-design.md`), not a
  replacement for it.
- No WebSocket-based CRUD, RAG search, or historical queries — those stay REST.
- No auth on the WS endpoint in this pass — the rest of the engine has no `spring-boot-starter-security`
  dependency and no JWT anywhere today (EMQX is anonymous-auth for dev); inventing WS-only auth would
  be inconsistent with the rest of the app's actual security posture. A documented seam is left for
  later (see Authentication below).
- No token-level LLM streaming for diagnosis — `DiagnosisAgentService.diagnose()` is a synchronous,
  blocking two-call inference with a retry-on-low-confidence branch; restructuring that into a
  streaming call is a separate, larger change and out of scope here. This design only pushes the
  *completion* event.

## Two facts from the existing pipeline that shape this design

1. **Device online/offline transitions never touch Kafka.** `DeviceHeartbeatService.heartbeat()` /
   `markOfflineNow()` / `markAllOffline()` / `onMessage()` (Redis key-expiry listener) write straight
   to `DeviceRepository` — no outbox row is created for these. So this flow needs a **direct
   in-process call**, not a Kafka consumer.
2. **`iot.device.data` and `iot.alert.triggered` each have exactly one consumer group today**
   (`iot.device.data` has none; `iot.alert.triggered` has only `AlertTriggeredConsumer`,
   groupId=`diagnosis-agent`). Kafka consumer groups are independent — a second consumer group on
   the same topic gets its own copy of every record without touching the existing consumer's offsets
   or behavior. This is how the WS bridge taps both topics without modifying `AlertTriggeredConsumer`,
   `TelemetryService`, or `AlertService`.

## Architecture

```
MQTT → MqttSubscriber → TelemetryService.process()
                            → Outbox(device.data) → Kafka iot.device.data
                                                        → AlertTriggeredConsumer          [unchanged, groupId=diagnosis-agent]
                                                        → TelemetryWsBridgeConsumer (NEW)  [groupId=ws-bridge]
                                                            → per-device coalescing buffer (~400ms)
                                                            → WsPushService.pushTelemetry(deviceKey, snapshot)
                                                            → /topic/telemetry/{deviceKey}

AlertService.evaluate() → Outbox(alert.triggered) → Kafka iot.alert.triggered
                                                        → AlertTriggeredConsumer            [unchanged] → DiagnosisAgentService.diagnose()
                                                        → AlertWsBridgeConsumer (NEW)        [groupId=ws-bridge]
                                                            → WsPushService.pushAlert(alertRecord)
                                                            → /topic/alerts

DiagnosisAgentService.writeBack()  → [NEW] WsPushService.pushDiagnosis(alertId, alertRecord)
                                        → /topic/diagnosis/{alertId}
                                    (direct call, no Kafka hop — writeBack() already has the
                                     fully-populated, just-saved AlertRecord in hand)

DeviceHeartbeatService.{heartbeat, markOfflineNow, markAllOffline, onMessage}
                                    → [NEW] WsPushService.pushDeviceStatus(event)
                                        → /topic/devices/status
                                    (direct call, no Kafka hop — see fact #1 above)
```

## Backend design

**Transport**: STOMP over SockJS at `/ws`, using Spring's `spring-boot-starter-websocket`
(`@EnableWebSocketMessageBroker`, `enableSimpleBroker("/topic")`). STOMP gives topic-based pub/sub
matching the Kafka topic model already in use, plus built-in heartbeats and a SockJS fallback — a raw
`WebSocketHandler` would mean hand-rolling subscription bookkeeping this app doesn't need.

Per this repo's Spring Boot 4 auto-configuration-module-split gotcha (see root `CLAUDE.md`): if
`SimpMessagingTemplate`/`@EnableWebSocketMessageBroker` don't wire up despite
`spring-boot-starter-websocket` on the classpath, add `org.springframework.boot:spring-boot-websocket`
explicitly, mirroring the `spring-boot-kafka` precedent.

**Topics**:
```
/topic/telemetry/{deviceKey}   — coalesced per-device telemetry snapshot
/topic/alerts                  — new alert created / alert record changed
/topic/diagnosis/{alertId}     — diagnosis complete (Diagnosed or HumanReviewRequired)
/topic/devices/status          — online/offline transitions, all devices on one topic (see below)
```

`/topic/devices/status` is a single shared topic (not per-device) because online/offline events are
low-frequency and every operator dashboard view (device table) wants all of them at once — there's no
per-device-detail-view use case for status the way there is for telemetry.

**Connection management**: no custom session registry needed — Spring's STOMP session lifecycle
(`SessionConnectEvent`/`SessionDisconnectEvent`) and `SimpUserRegistry` cover connection-count
observability if needed later. No manual bookkeeping in this design.

**Authentication (future-ready seam)**: none in this pass. When needed, a `ChannelInterceptor` on the
inbound STOMP channel inspecting the `CONNECT` frame's `Authorization` header is the standard Spring
pattern — it can be added later without touching topic routing or any of the classes below.

**Origin allowlist (added after automated security review)**: `WebSocketConfig` restricts the `/ws`
handshake to an exact origin allowlist (`app.ws-allowed-origins`, defaulting to the frontend dev
origin `http://localhost:3000`) rather than a wildcard. This is not the same gap as "no auth" above —
a WebSocket handshake isn't subject to the browser's normal same-origin CORS enforcement the way REST
`fetch`/XHR is, so `setAllowedOriginPatterns("*")` would let *any* origin open a connection and
receive live telemetry/alert/diagnosis pushes, which is strictly worse than the REST API's implicit
same-origin protection today. Restricting to a known origin list is cheap and unambiguous, unlike
adding real authentication, so it's done now rather than deferred.

## Event routing (concrete class map)

| Source | Hook point | Path | WS topic |
|---|---|---|---|
| MQTT telemetry | `TelemetryService.process()` | Outbox → Kafka `iot.device.data` → **new** `TelemetryWsBridgeConsumer` (groupId=`ws-bridge`) | `/topic/telemetry/{deviceKey}` |
| Alert rule match / manual re-trigger | `AlertService.evaluate()`, `AlertService.requestDiagnosis()` | Outbox → Kafka `iot.alert.triggered` → **new** `AlertWsBridgeConsumer` (groupId=`ws-bridge`) | `/topic/alerts` |
| Diagnosis complete | `DiagnosisAgentService.writeBack()` | direct call, right after `alertRecordRepository.save(record)` | `/topic/diagnosis/{alertId}` |
| Device online/offline | `DeviceHeartbeatService` (4 transition points) | direct call at each transition | `/topic/devices/status` |

## Stability rules (revised after code review — see below)

- **Backpressure**: bound the `clientOutboundChannel` executor's queue so one slow browser tab can't
  back up the JVM; a full queue drops that client's own messages rather than blocking senders.
- **Per-device telemetry coalescing**: `TelemetryWsBridgeConsumer` buffers incoming per-property
  `device.data` records per `deviceKey` (`ConcurrentHashMap<String, ConcurrentHashMap<String,Object>>`)
  and flushes each device's accumulated snapshot on a `~400ms` scheduled tick, sending **one** message
  per device per tick to that device's own topic — not one message per property, and not one message
  per Kafka record. This bounds telemetry load to O(devices with changes in that window), not
  O(properties).
- **Alert/diagnosis messages are never throttled or coalesced** — they're low-frequency and
  operationally significant; only telemetry gets rate-limited.
- **Deduplication / ordering — the actual fix for the race identified in review**: two independent
  async paths write to the same `AlertRecord` over time (`AlertWsBridgeConsumer` via Kafka, and the
  direct `writeBack()` call), with no relative ordering guarantee between them. If `AlertWsBridgeConsumer`
  lags, a client could receive a stale "alert.triggered" snapshot *after* it already received the
  newer diagnosis-complete snapshot, and regress the displayed status. Fix: every alert/diagnosis WS
  payload is the `AlertRecord` entity itself (or a projection of it), which already carries
  `BaseEntity.updateTime` (auto-bumped by `@PreUpdate` on every save, not `@JsonIgnore`'d — verified in
  `BaseEntity.java`). The frontend keys its per-alert cache by the newest `updateTime` seen and ignores
  any incoming message with an older one. No new version field needs to be added anywhere in the
  engine — this falls out of the existing entity as-is.
- **Reconnect**: client does one REST fetch on connect/reconnect to resync full state, then applies WS
  messages as incremental deltas on top. REST is the source of truth for "what did I miss while
  disconnected" — no Kafka replay mechanism is needed at the WS layer.

### Corrected from the original draft

The original draft also proposed batching *multiple devices'* deltas into a single message per broadcast
tick. That's dropped: it's inconsistent with keeping per-device destinations
(`/topic/telemetry/{deviceKey}`), which was deliberately kept (over a single global `/topic/telemetry`
topic) so a client viewing one device's detail page only receives that device's frames, not all
devices'. Cross-device batching only makes sense together with a single shared destination, which this
design does not use for telemetry. The per-device coalescing already described is the correct and
sufficient mitigation for message-storm risk under a per-device-topic model.

### On Spring's `SimpleBroker` and topic count

`SimpleBroker` keeps subscriptions as an in-memory `destination → sessions` map — a "topic" is just a
string key, not a Kafka-style partitioned/persistent resource. Even a few thousand distinct
`/topic/telemetry/{deviceKey}` destinations is a trivial (sub-MB) memory cost, not a scaling risk at
this workspace's actual scale (`spark-agent-simulator` emulates 6 devices). `SimpleBroker`'s real
limitation is horizontal scaling: subscriptions live in one JVM's memory, so a message published on
engine instance A never reaches a client connected to instance B. That's solved with an external STOMP
relay (RabbitMQ/ActiveMQ) or Redis pub/sub fan-out if/when the engine runs multi-instance — noted here
as a future option, not implemented now.

## File-level impact

**New**: `config/WebSocketConfig.java`, `ws/WsPushService.java`, `ws/DeviceStatusEvent.java`,
`kafka/TelemetryWsBridgeConsumer.java`, `kafka/AlertWsBridgeConsumer.java`, plus matching test files.

**Modified (hook-only, no logic changes)**: `DiagnosisAgentService.writeBack()` (one call after
save), `DeviceHeartbeatService` (one call at each of 4 transition points), `build.gradle` (add
`spring-boot-starter-websocket`).

**Untouched**: `AlertTriggeredConsumer`, `OutboxRelayService`, `TelemetryService`, `AlertService`,
all repositories, all existing REST controllers.
