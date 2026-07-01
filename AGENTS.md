# AGENTS.md — spark-agent-engine

Java 25 / Spring Boot 4.1.0 / Gradle 9.5. Single-module IoT telemetry ingestion + AI diagnosis engine.

Package root: `com.spark.agent` — source is `src/main/java/com/spark/agent/`.

## Commands (always `./gradlew`, never system gradle)

```bash
./gradlew build          # compile + test
./gradlew build -x test  # compile only (faster)
./gradlew bootRun        # port 8080; if taken: --args='--server.port=8081'
./gradlew test --tests "com.spark.agent.SomeTest"
./gradlew assemble       # JAR without tests
```

**Pre-build required**: Docker containers for EMQX, PostgreSQL, Redis, Kafka must be running (see README.md `docker run` commands). Integration tests and `bootRun` will fail without them.

## Spring Boot 4 hard-won facts (will cause build failures if missed)

- **Jackson 3**: import `tools.jackson.databind.ObjectMapper`, NOT `com.fasterxml.jackson.databind`. Annotations (`@JsonProperty` etc.) stay at `com.fasterxml.jackson.annotation` — unchanged.
- **Kafka auto-config**: requires explicit `implementation 'org.springframework.boot:spring-boot-kafka'` in `build.gradle` (Boot 4 split auto-config into per-module jars).
- **KafkaTemplate generic**: Boot 4 gives `KafkaTemplate<Object, Object>`, inject as that, not `<String, String>`.
- **`spring-boot-starter-validation` is NOT on classpath** — `@Valid` / `@NotBlank` etc. are silently ignored. Add it to `build.gradle` before using any validation annotations.
- **Date serialization config**: use `spring.jackson.datatype.datetime.write-dates-as-timestamps: false`. The old `spring.jackson.serialization.write-dates-as-timestamps` throws a bind error at startup.
- **No `@EnableKafka`** needed — the `spring-boot-kafka` auto-config includes `KafkaAnnotationDrivenConfiguration` which provides it. If Kafka listeners don't fire, add `@EnableKafka` on `KafkaConsumerConfig` as a diagnostic.
- HikariCP: `auto-commit: false` — `@Transactional` owns all commit boundaries.
- JPA: `ddl-auto: none`, `open-in-view: false`.

## Architecture

```
MQTT (EMQX :1883) → MqttSubscriber → TelemetryService (@Transactional)
  ├─ DeviceHeartbeatService.heartbeat()    Redis SET NX EX; DB write only on offline→online
  ├─ DeviceDataRepository.saveAll()        batch insert, snowflake IDs
  ├─ KafkaProducerService.sendTelemetry()  one per property row (fire-and-forget)
  └─ AlertService.evaluate()               per property row
       ├─ rule matching (gt/lt/gte/lte/eq/ne, threshold is VARCHAR)
       ├─ debounce check (countRecentUnhandled)
       └─ KafkaProducerService.sendAlert()

Redis key expiry (__keyevent@0__:expired) → DeviceHeartbeatService.onMessage()
  └─ DeviceRepository.markOffline()

REST /api (ApiController) → DeviceDataRepository / AlertRecordRepository
MCP tools (DeviceMcpToolService) → same repos + RagSearchService
```

### Critical data flow note

`TelemetryService.process()` is `@Transactional` and sends Kafka messages inside the transaction. Kafka send is fire-and-forget (CompletableFuture never joined). If the DB rolls back after Kafka send, the Kafka message is already out (at-most-once). **This is a known design limitation**, acceptable for phase 1. Do not "fix" without discussing — the planned upgrade is transactional outbox in phase 2.

## Schema management

- **`ddl-auto: none`** — schema never touched by Hibernate. All table DDL is managed by the external `spark-iot-agent` management system.
- **Composite index `idx_device_data_key_identifier_time`** on `(device_key, identifier, report_time DESC) WHERE deleted = 0` must be created manually on any new environment (see `sql/2026-07-01-device-data-index.sql`). Without it, the `findLatestByDeviceKey` query does full table scans (~68s at 80k rows).
- **Snowflake IDs** — all PKs generated via `SnowflakeIdGenerator` (machineId hardcoded to 1). Tables use `bigint` PK with no DB auto-increment.

## Known bugs (don't reintroduce)

These exist in the current codebase. When modifying these files, do not remove the guard that would be needed, and if you see a chance to fix them, flag it:

1. **`AlertTriggeredConsumer.java:30`**: `diagnose()` called unconditionally after `readValue()` exception — no `return` in the catch block. A parse failure will call `diagnose(payload)` with the raw string that just failed to parse.
2. **`MqttSubscriber.java`**: No `@PreDestroy` — client never disconnects on shutdown (Netty threads + TCP connection leak).
3. **`MqttSubscriber.java:71`**: HiveMQ callback runs on Netty's event loop thread, but `TelemetryService.process()` does blocking DB I/O. Under load this stalls MQTT keep-alive → EMQX drops the connection.
4. **`KafkaProducerService.java:37-42`**: Fire-and-forget send (`thenAccept`/`exceptionally` return null). Serialization failures logged and swallowed. Caller gets no signal.
5. **`SnowflakeIdGenerator.java:22`**: `machineId` hardcoded to 1 — deploying a second instance causes ID collisions. Clock rollback resets sequence to 0, also causing duplicates.
6. **`DiagnosisAgentService.java:137-143`**: `chatClient.prompt().call()` has no timeout — a stuck Ollama call blocks the Kafka listener thread pool indefinitely.
7. **`KnowledgeIngestionService.java:31`**: `ingest()` lacks `@Transactional` — embedding failures leave orphaned `knowledge` rows.
8. **`VectorStoreRepository.java:22-25`**: `saveEmbedding()` is an `UPDATE` — silently succeeds with 0 rows affected if the row doesn't exist.
9. **No `@RestControllerAdvice`** exists — all exceptions produce Spring's default error response instead of `R<T>`.
10. **Redis password mismatch**: `application.yaml` sets `password: redis123456` but the dev Docker instance has no `--requirepass`. Heartbeat/offline detection silently non-functional unless you align them.

## REST API

All return `{"code": 0, "msg": "success", "data": [...]}` via `R<T>`.

```
GET /api/device/{deviceKey}/latest
GET /api/device/{deviceKey}/history?identifier=<id>&limit=<n>      (default 50)
GET /api/alert/recent?limit=<n>                                      (default 20)
POST /api/knowledge/import           body: List<KnowledgeImportItem> (unbounded!)
POST /api/rag/ingest                 body: IngestRequest
POST /api/rag/search                 body: SearchRequest
```

**Gotchas**: No validation, no auth, no rate limiting. `R.fail()` always returns code 500. All endpoints return JPA entities directly (exposing `deleted`, `tenantId`, etc.).

## MCP layer

Spring AI MCP server via `spring-ai-starter-mcp-server-webmvc`. Tools are registered via `MethodToolCallbackProvider` in `McpToolConfig`. Available tools (4):

- `queryDeviceStatus(deviceKey)` → `DeviceStatusResult`
- `queryDeviceHistory(deviceKey, identifier, hours)` → `List<DeviceData>` (capped at 500 rows)
- `queryDeviceAlerts(deviceKey, limit)` → `List<AlertRecord>` (capped at 500)
- `queryDeviceManual(deviceModel, question)` → RAG search results

MCP config in `application.yaml`: `spring.ai.mcp.server.type: SYNC, protocol: STREAMABLE`.

## AI / Diagnosis

- **Chat model**: `qwen2.5:7b` via Ollama at `http://localhost:11434`
- **Embedding model**: `nomic-embed-text` via Ollama
- **Vector store**: pgvector on `aiot_knowledge.embedding` column
- **Diagnosis flow**: `AlertTriggeredConsumer` → `DiagnosisAgentService.diagnose()` → `ChatClient.call()` → `writeBack()` to `aiot_alert_record`
- **Reflection retry**: if confidence < 40% or no RAG results found, retries once with widened telemetry window (120 min)
- `app.diagnosis-confidence-threshold: 80` — at/above this, `diagnosis_status=2` (auto); below, `diagnosis_status=1` (human review)

## Important config values

| Key | Value | Notes |
|---|---|---|
| `spring.jpa.hibernate.ddl-auto` | `none` | Never change; schema is external |
| `spring.jpa.open-in-view` | `false` | Lazy loading will throw without active tx |
| `spring.datasource.hikari.auto-commit` | `false` | |
| `kafka.topic.alert-triggered` | `iot.alert.triggered` | 6 partitions, concurrency=6 listeners |
| `app.device-heartbeat-ttl-seconds` | `30` | ~3× device reporting interval |
| `app.alert-debounce-minutes` | `5` | Debounce window for duplicate alerts |
| `spring.ai.ollama.chat.model` | `qwen2.5:7b` | Must be available in local Ollama |
| `spring.ai.ollama.embedding.model` | `nomic-embed-text` | Must be available in local Ollama |

## Infrastructure

| Service | Host:Port | Docker container |
|---|---|---|
| EMQX | localhost:1883 | spark-emqx |
| PostgreSQL | localhost:5432 (db: spark_ai, user/pass: root/root123456) | spark-postgres |
| Kafka | localhost:29092 (internal 9092) | spark-kafka |
| Redis | localhost:6379 (no auth in Docker, but yaml has password set) | spark-redis |
| Ollama | localhost:11434 | (bare metal or docker) |

Verify Kafka topics: `docker exec spark-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic iot.device.data`

## DB entity quirks

- `BaseEntity`: `deleted` is `Short` (`0` = active), `tenantId` defaults to `1L`, `createTime`/`updateTime` set via `@PrePersist`/`@PreUpdate`.
- `aiot_alert_rule.threshold` is `VARCHAR(64)`, parsed as `Double` at runtime in `AlertService.matches()`.
- Hibernate naming: camelCase → snake_case (Spring default). No explicit `@Column(name)` needed unless the field name diverges from this pattern.
- All IDs are `Long` (`bigint`), never auto-increment — always via `SnowflakeIdGenerator.nextId()`.

## Testing

Single `contextLoads()` test exists. The project is effectively untested. Adding tests has high value.
