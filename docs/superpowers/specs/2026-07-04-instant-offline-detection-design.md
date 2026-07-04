# Instant Device Connectivity Events (online/offline/status)

## Context

`spark-iot-emulator` publishes explicit device connectivity events that
`spark-agent-engine` currently ignores entirely:

- `device/online/{deviceKey}` — device resumed reporting (`online <device>` command)
- `device/offline/{deviceKey}` — device stopped reporting (`offline <device>` command)
- `device/offline/emulator` — MQTT Last Will and Testament, published by the broker
  when the whole emulator process crashes/disconnects uncleanly
- `/sys/{productKey}/{deviceKey}/thing/event/status/post` — periodic uptime status,
  published on a separate cadence from telemetry

Today, `MqttSubscriber` subscribes to exactly one topic filter
(`mqtt.topic`, default `/sys/+/+/thing/event/property/post`). Online/offline state
is derived purely from `DeviceHeartbeatService`'s Redis `SETNX+EX` heartbeat: a
device is marked offline only when its Redis key's TTL (default 30s) expires and
the keyspace-notification listener fires. This means:

- A device going offline via the emulator's `offline` command isn't reflected in
  `aiot_device.online_status` until the TTL window elapses.
- If the whole emulator process crashes, every device it manages goes stale
  independently, each waiting out its own TTL, instead of being caught instantly by
  the LWT signal the emulator already publishes for exactly this case.
- The status/post uptime heartbeat is dropped on the floor — never consumed.

This spec adds MQTT subscriptions for these three additional topics so device
connectivity state reacts to explicit signals immediately, while keeping the
existing Redis TTL mechanism as a fallback for the case where no explicit event
arrives (e.g., network partition without a clean disconnect).

## Goals

- React to `device/online/{deviceKey}` and `device/offline/{deviceKey}` immediately
  instead of waiting on TTL expiry.
- React to the `device/offline/emulator` LWT by marking **all** currently-online
  devices offline at once, instead of waiting for each device's TTL to expire
  independently.
- Consume `thing/event/status/post` as an additional heartbeat signal (refresh
  Redis TTL / trigger online transition), without persisting the uptime value.

## Non-goals

- Persisting uptime numbers anywhere (no schema change).
- Consuming `thing/event/alert/post` (device-side alerts) — the engine already
  computes its own alerts from raw telemetry via `AlertService`; this topic is out
  of scope.
- Changing or removing the existing Redis TTL heartbeat mechanism — it remains the
  fallback for silent disconnects that produce no explicit MQTT event.

## Architecture

The existing `Mqtt5AsyncClient` in `MqttSubscriber` gains three more subscriptions,
each with its own callback, alongside the existing telemetry subscription:

```
Mqtt5AsyncClient (unchanged, still one connection)
  ├─ mqtt.topic        (existing) → dispatch()        → TelemetryService.process()
  ├─ device/online/+   (new)      → dispatchOnline()  → DeviceHeartbeatService.heartbeat()
  ├─ device/offline/+  (new)      → dispatchOffline() → markOfflineNow() or markAllOffline()
  └─ status topic      (new)      → dispatchStatus()  → DeviceHeartbeatService.heartbeat()
```

`device/offline/+` uses a single-level MQTT wildcard, which matches both
`device/offline/{deviceKey}` and the LWT topic `device/offline/emulator` — one
subscription covers both cases. The two are distinguished at the payload level:
`deviceKey == "emulator"` routes to `markAllOffline()`; any other value routes to
`markOfflineNow(deviceKey)`.

All four topics share the existing `mqttExecutor` (virtual-thread-per-task); a
failure handling one topic's message never blocks or affects the others.

### Why extend `MqttSubscriber` instead of a new component

Considered a separate `DeviceLifecycleSubscriber` with its own `Mqtt5AsyncClient`.
Rejected: these events are the same underlying concern (device connectivity) as the
telemetry stream, and a second client would duplicate all the connection-management
boilerplate (reconnect listener, client id generation, disconnect logging) for no
real isolation benefit — the four topics are cheap, low-volume, and already
naturally partitioned by callback.

## Components

**`MqttProperties`** — three new configurable fields, defaulting to the fixed
protocol topics documented in the emulator's README:

```java
private String onlineTopic = "device/online/+";
private String offlineTopic = "device/offline/+";
private String statusTopic = "/sys/+/+/thing/event/status/post";
```

**New DTOs** (both `@JsonIgnoreProperties(ignoreUnknown = true)` so payload field
additions don't break deserialization):

```java
// shared shape for device/online and device/offline payloads
record DeviceConnectionEventMessage(String deviceKey, String status, long timestamp) {}

// status/post: only deviceKey is consumed; uptime is intentionally not modeled
record DeviceStatusEventMessage(String deviceKey) {}
```

**`DeviceHeartbeatService`** — two new methods:

- `markOfflineNow(String deviceKey)`: deletes the device's Redis heartbeat key,
  looks up the device by key, and calls `deviceRepository.markOffline(id, now)`
  immediately (no TTL wait). `markOffline` is an unconditional UPDATE, so calling it
  on an already-offline device is a harmless no-op.
- `markAllOffline()`: for the emulator-crash case — queries all devices with
  `onlineStatus == 1` (new `DeviceRepository.findByOnlineStatusAndDeleted` query),
  and for each: deletes its Redis key and calls `markOffline`.

**Online and status events reuse the existing `heartbeat(deviceId, deviceKey)`**:
look up the device by key, then call the same method telemetry already calls —
same `SETNX+EX` / online-transition logic, no new code path.

## Data Flow

1. **Online** (`device/online/{deviceKey}`): emulator's `online DK_INJ_003` command
   publishes `{deviceKey, status:"online", timestamp}` → `dispatchOnline()` parses →
   looks up device by key → if found, calls `heartbeat(id, deviceKey)` (transition
   write only if it was offline) → if not found, logs and skips.

2. **Offline** (`device/offline/{deviceKey}`): emulator's `offline DK_INJ_003`
   command publishes the same shape with `status:"offline"` → `dispatchOffline()`
   parses → `deviceKey != "emulator"` → looks up device → `markOfflineNow(deviceKey)`.

3. **Emulator crash** (LWT, same `device/offline/+` subscription): broker detects
   an unclean disconnect → publishes `{deviceKey:"emulator", status:"offline",
   timestamp}` to `device/offline/emulator` → matches the same subscription →
   `dispatchOffline()`'s `deviceKey.equals("emulator")` branch → `markAllOffline()`,
   marking every currently-online device offline and clearing their Redis keys in
   one pass.

4. **Status heartbeat** (`.../thing/event/status/post`): periodic uptime report →
   `dispatchStatus()` extracts only `deviceKey` → if found, calls the same
   `heartbeat()` as telemetry — refreshes the Redis TTL and triggers an online
   transition if the device was previously marked offline. The uptime number itself
   is not parsed or persisted.

## Error Handling

Mirrors the existing `dispatch()` pattern exactly — no new error-handling model:

- Each new handler wraps its body in `try { ... } catch (Exception e) { log.error(...); }`;
  nothing is ever rethrown or allowed to block other `mqttExecutor` tasks.
- Malformed JSON is logged and skipped, matching the existing
  `dispatch_invalidJson_doesNotThrowAndSkipsTelemetryService` behavior.
- An unknown `deviceKey` (not in `aiot_device`) is logged and skipped, matching
  `TelemetryService.process_unknownDevice_doesNothing`.
- If `redisTemplate.delete(...)` inside `markOfflineNow`/`markAllOffline` throws
  (e.g., Redis unavailable), it's caught and logged by the same outer try/catch;
  the DB-side `markOffline` write is a separate operation and isn't rolled back by
  a Redis failure — this path is purely a latency optimization on top of the
  existing TTL mechanism, which still catches the case eventually.

## Testing

- `MqttSubscriberTest`: valid-message / invalid-JSON / unknown-device cases for
  each of the three new handlers, plus one case asserting that an offline payload
  with `deviceKey == "emulator"` calls `markAllOffline()` instead of
  `markOfflineNow()`.
- `DeviceHeartbeatServiceTest`: `markOfflineNow` for an online device (marks
  offline + deletes key) and an already-offline device (safe no-op);
  `markAllOffline` with multiple online devices (all marked offline) and with none
  online (no-op).
- `DeviceRepositoryTest`: coverage for the new `findByOnlineStatusAndDeleted` query,
  consistent with existing repository test style.

## Configuration Reference Update

`CLAUDE.md`'s MQTT config table should gain the three new topic properties once
implemented, alongside the existing `mqtt.topic`.
