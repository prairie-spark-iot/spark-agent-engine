# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Stack

- **Java 25** + **Spring Boot 4.1.0** (Gradle wrapper `./gradlew`)
- **Jackson 3.x** — package root changed from `com.fasterxml.jackson` → `tools.jackson` (e.g. `tools.jackson.databind.ObjectMapper`). Annotations remain at `com.fasterxml.jackson.annotation`.
- **Spring Boot 4 module split** — Kafka auto-configuration lives in `spring-boot-kafka` (must be declared explicitly). Same pattern will apply to other non-web auto-configurations.
- Package root: `com.spark.agent`

## Commands

```bash
# Build
./gradlew build

# Run (port 8080; if 8080 is taken use --args='--server.port=8081')
./gradlew bootRun

# Test (all)
./gradlew test

# Single test class
./gradlew test --tests "com.spark.agent.SparkAgentEngineApplicationTests"

# Assemble JAR without tests
./gradlew assemble
```

## Architecture

```
MQTT (EMQX) → MqttSubscriber → TelemetryService ─┬─→ DeviceDataRepository (aiot_device_data)
                                                   ├─→ DeviceRepository.markOnline (aiot_device)
                                                   ├─→ KafkaProducerService → iot.device.data
                                                   └─→ AlertService ─┬─→ AlertRecordRepository (aiot_alert_record)
                                                                      └─→ KafkaProducerService → iot.alert.triggered

DeviceStatusService @Scheduled → DeviceRepository.markOfflineBatch (offline sweep)

ApiController → DeviceDataRepository / AlertRecordRepository
```

### Key packages

| Package | Role |
|---|---|
| `mqtt` | HiveMQ async client, subscribes `/sys/+/+/thing/event/property/post` |
| `service` | `TelemetryService` (core pipeline), `AlertService` (rules + debounce), `DeviceStatusService` (offline sweep) |
| `kafka` | `KafkaProducerService` wraps `KafkaTemplate<Object,Object>` |
| `entity` / `repository` | JPA entities + Spring Data repos for 4 tables; `BaseEntity` carries ruoyi audit fields |
| `common` | `SnowflakeIdGenerator` (IDs for all inserts), `R<T>` (API response wrapper) |
| `config` | `MqttProperties`, `AppProperties` (`@ConfigurationProperties`) |
| `controller` | `ApiController` — 3 read-only GET endpoints |

### Entity ID strategy

All IDs are snowflake-generated (`SnowflakeIdGenerator`). Tables use `bigint` with no DB auto-increment.

### Alert debounce

`AlertService` skips inserting a new `aiot_alert_record` if the same device + rule already has an unhandled record within `app.alert-debounce-minutes` (default 5 min).

## Infrastructure (Docker, already running)

| Service | Address |
|---|---|
| EMQX (MQTT) | `localhost:1883`, anonymous auth |
| PostgreSQL | `localhost:5432`, db `spark_ai`, user `root` / `root123456` |
| Kafka | `localhost:29092` (host-mapped port) |

## REST API

```
GET /api/device/{deviceKey}/latest
GET /api/device/{deviceKey}/history?identifier=temperature&limit=50
GET /api/alert/recent?limit=20
```

All return `{code: 0, msg: "success", data: [...]}`.

## Configuration knobs (application.yaml)

```yaml
mqtt.host / .port / .topic / .client-id-prefix
app.offline-timeout-seconds   # default 60
app.alert-debounce-minutes    # default 5
kafka.topic.device-data       # iot.device.data
kafka.topic.alert-triggered   # iot.alert.triggered
```
