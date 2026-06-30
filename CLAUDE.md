# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Stack

- **Java 25** + **Spring Boot 4.1.0** + **Gradle 9.5** (use `./gradlew`, not system gradle)
- Package root: `com.spark.agent`

## Commands

```bash
./gradlew build          # compile + test
./gradlew build -x test  # compile only (faster)
./gradlew bootRun        # run on :8080 (port 8080 may be taken — use --args='--server.port=8081')
./gradlew test --tests "com.spark.agent.SomeTest"  # single test class
./gradlew assemble       # JAR without tests
```

## Spring Boot 4 Gotchas (read before touching deps or imports)

These caused real build failures during development:

**1. Jackson 3.x — package rename AND feature rename**
Spring Boot 4 ships Jackson 3.x. Two changes matter:

*Package rename:*
- Use `tools.jackson.databind.ObjectMapper` (not `com.fasterxml.jackson.databind`)
- Use `tools.jackson.core.*` for core types
- Annotations (`@JsonProperty`, `@JsonIgnore`, etc.) remain at `com.fasterxml.jackson.annotation` — unchanged

*Date feature moved:* `SerializationFeature.WRITE_DATES_AS_TIMESTAMPS` no longer exists. Use:
```yaml
spring:
  jackson:
    datatype:
      datetime:
        write-dates-as-timestamps: false   # ISO-8601 dates in REST responses
```
`spring.jackson.serialization.write-dates-as-timestamps` will throw a bind error on startup.

**2. Spring Boot 4 auto-configuration module split**
Boot 4 extracted each auto-configuration into its own module. If you add a new integration (Redis, RabbitMQ, etc.) and the bean is not found despite having the client on the classpath, add the corresponding `spring-boot-<name>` module explicitly:
```groovy
// Example — Kafka required this:
implementation 'org.springframework.boot:spring-boot-kafka'
// Web, JPA, etc. are already covered by their starters
```

**3. `KafkaTemplate` generic type**
Boot 4 auto-configures `KafkaTemplate<Object, Object>`, not `KafkaTemplate<String, String>`. Inject as `KafkaTemplate<Object, Object>`.

**4. JDK 25 Lombok warning (harmless)**
`sun.misc.Unsafe::objectFieldOffset` deprecation warning from Lombok at compile time. Not an error, ignore it.

## Architecture

```
MQTT (EMQX :1883)
  └─► MqttSubscriber          HiveMQ async client; reconnects automatically;
        │                     re-subscribes via addConnectedListener on each connect
        ▼
      TelemetryService         @Transactional; one call per MQTT message
        ├─► DeviceRepository.markOnline()         JPQL UPDATE, no entity load
        ├─► DeviceDataRepository.saveAll()        batch insert, snowflake IDs
        ├─► KafkaProducerService.sendTelemetry()  one message per property row
        └─► AlertService.evaluate()               per property row
              ├─► AlertRuleRepository.findActiveRules()
              ├─► AlertRecordRepository.countRecentUnhandled()  debounce
              ├─► AlertRecordRepository.save()
              └─► KafkaProducerService.sendAlert()

DeviceStatusService  @Scheduled every (offlineTimeoutSeconds/2) ms
  └─► DeviceRepository.markOfflineBatch()

ApiController  GET endpoints → DeviceDataRepository / AlertRecordRepository
```

## Key Design Decisions

**Snowflake IDs** — all `INSERT`s generate IDs via `SnowflakeIdGenerator` (machine ID hardcoded to 1). Tables have `bigint` PK with no DB auto-increment.

**Alert debounce** — `AlertService` calls `countRecentUnhandled(deviceId, ruleId, since)` before inserting. If count > 0, the alert is skipped. Window is `app.alert-debounce-minutes` (default 5 min).

**Kafka send inside transaction** — `KafkaProducerService` sends within the `@Transactional` scope of `TelemetryService`. If the DB transaction rolls back, the Kafka message is already sent (at-most-once). Acceptable for phase 1; upgrade to transactional outbox in phase 2 if needed.

**`aiot_alert_rule.threshold` is VARCHAR** — the DB column is `character varying(64)`, not numeric. `AlertService.matches()` parses it as `Double` at runtime.

**`tenant_id` default** — `BaseEntity` defaults `tenantId = 1L` (matches existing data in the DB; the DB column default is 0 but all inserted rows use 1).

## Entity / DB Field Mapping Reference

Hibernate naming: Java camelCase → SQL snake_case (Spring default). No explicit `@Column(name)` needed unless field name diverges from this pattern.

| Java type | DB type |
|---|---|
| `Long` | `bigint` |
| `Short` | `smallint` |
| `BigDecimal` | `numeric(20,4)` |
| `LocalDateTime` | `timestamp without time zone` |
| `String` | `character varying` / `text` |

`BaseEntity` carries: `creator`, `createTime`, `updater`, `updateTime`, `deleted` (short, 0=active), `tenantId`. `@PrePersist` / `@PreUpdate` set timestamps automatically.

## Infrastructure (Docker, should already be running)

| Service | Host:Port | Notes |
|---|---|---|
| EMQX | `localhost:1883` | anonymous auth, MQTT 5 |
| PostgreSQL | `localhost:5432` | db `spark_ai`, user `root` / `root123456` |
| Kafka | `localhost:29092` | host-mapped port (internal is 9092) |

Verify Kafka topics: `docker exec spark-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic iot.device.data`

## Configuration Reference (`application.yaml`)

```yaml
mqtt:
  host: localhost          # EMQX host
  port: 1883
  topic: "/sys/+/+/thing/event/property/post"
  client-id-prefix: spark-agent

app:
  offline-timeout-seconds: 60    # device marked offline after this long without data
  alert-debounce-minutes: 5      # suppress duplicate alerts within this window

kafka:
  topic:
    device-data: iot.device.data
    alert-triggered: iot.alert.triggered

spring.kafka.bootstrap-servers: localhost:29092
spring.jpa.hibernate.ddl-auto: none   # never let Hibernate touch the schema
```

## REST API

```
GET /api/device/{deviceKey}/latest
    → latest value per identifier for the device

GET /api/device/{deviceKey}/history?identifier=<id>&limit=<n>
    → history for one property, newest first, default limit 50

GET /api/alert/recent?limit=<n>
    → recent alert records, newest first, default limit 20
```

All return `{"code": 0, "msg": "success", "data": [...]}` via `R<T>`.

## MQTT Message Format (DeviceTelemetryMessage)

```json
{
  "deviceKey": "DK_INJ_001",
  "productKey": "PK_INJECTION_MA",
  "timestamp": 1719655200000,
  "properties": {
    "temperature": 235.5,
    "pressure": 156.2,
    "current": 45.8
  }
}
```

Each key in `properties` becomes one `aiot_device_data` row. Values are `Number` subtypes (parsed by Jackson 3 from JSON numbers).

## Future Phases (hook points)

- **Phase 2 — AI diagnosis**: consume `iot.alert.triggered`; call LLM; write back `aiot_alert_record.root_cause / suggestion / confidence / diagnosis_status=1`. Columns already exist in the table.
- **Phase 3 — RAG**: add vector store beside `AlertService`; enrich prompt with relevant manual excerpts before calling LLM.
- **Phase 4 — MCP**: expose device query and alert management as MCP tools for external AI agents.
