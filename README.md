# spark-agent-engine

工业 IoT 设备智能运维平台核心微服务——**AI 诊断引擎 + 设备数据接入网关**。

本仓库为**阶段一：数据接入网关**，完整打通从设备到数据库再到消息队列的链路：

```
设备模拟器 → EMQX (MQTT) → 本服务 → PostgreSQL (持久化)
                                    → Kafka (流转 & AI 触发)
```

后续阶段（RAG 知识库、AI 诊断 Agent、MCP 接口）将以本服务的 Kafka 消息为触发入口。

---

## 技术栈

| 层次 | 技术 |
|---|---|
| 运行时 | Java 25 + Spring Boot 4.1.0 |
| 构建 | Gradle 9.5（Groovy DSL，项目自带 wrapper） |
| MQTT 客户端 | HiveMQ MQTT Client 1.3.15（MQTT 5.0，异步 API） |
| 消息队列 | Apache Kafka（Spring for Apache Kafka 4.x） |
| 持久化 | Spring Data JPA + Hibernate 7.4 + PostgreSQL |
| JSON | Jackson 3.x（Boot 4 随附，包名 `tools.jackson.*`） |
| 辅助 | Lombok |

---

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                   spark-agent-engine                     │
│                                                          │
│  MqttSubscriber ──► TelemetryService                    │
│  (HiveMQ async)      │                                   │
│                      ├─► DeviceHeartbeatService         │
│                      │     ├── Redis SETNX+EX           │
│                      │     │   device:online:{key}      │
│                      │     └── DeviceRepository         │
│                      │         markOnline (↑ only)      │
│                      │                                   │
│                      ├─► DeviceDataRepository           │
│                      │   (saveAll → aiot_device_data)   │
│                      │                                   │
│                      ├─► KafkaProducerService           │
│                      │   (→ iot.device.data)            │
│                      │                                   │
│                      └─► AlertService                   │
│                          ├─► AlertRuleRepository        │
│                          │   (rule matching + debounce) │
│                          ├─► AlertRecordRepository      │
│                          │   (→ aiot_alert_record)      │
│                          └─► KafkaProducerService       │
│                              (→ iot.alert.triggered)    │
│                                                          │
│  Redis key expiry ──► DeviceHeartbeatService            │
│  (keyspace notify)    └── DeviceRepository              │
│                           markOffline (↓ only)          │
│                                                          │
│  ApiController ──► DeviceDataRepository                 │
│  (REST GET)        AlertRecordRepository                 │
└─────────────────────────────────────────────────────────┘
```

---

## 功能特性

### 1. MQTT 消费（稳定可靠）
- 订阅 `/sys/+/+/thing/event/property/post`，QoS 1
- HiveMQ 异步客户端 + `automaticReconnect`，断线自动重连并重订阅
- 单条消息异常 catch 后记日志继续，不影响后续消息

### 2. 遥测数据入库
- 每条 MQTT 消息的 `properties` 拆成多行写入 `aiot_device_data`
- `value`（文本）和 `value_num`（`numeric(20,4)`）双列存储，便于后续聚合
- ID 由内置雪花算法生成（41 位时间戳 | 10 位机器 | 12 位序列）

### 3. 设备在线状态维护（Redis 事件驱动）
- 收到消息时，`DeviceHeartbeatService` 向 Redis 写入 `device:online:{deviceKey}`，TTL 可配置（默认 30s，约 3 个上报周期）
- **状态变更才写库**：仅当 Redis 中该 key 不存在（离线→在线）时才 UPDATE 数据库，设备持续在线时只刷新 Redis TTL，不产生数据库写入
- **事件驱动离线检测**：Redis key 过期时通过键空间通知（`notify-keyspace-events=Ex`）触发 `onMessage`，仅在此时写 `online_status=0`，无需定时轮询

### 4. 告警规则引擎
- 支持 6 种比较运算符：`gt / lt / gte / lte / eq / ne`
- 规则可绑定具体设备，也可作用于所有设备（`device_id` 为 NULL）
- **告警防抖**：同一设备 + 同一规则在 `app.alert-debounce-minutes`（默认 5 分钟）内有未处置告警时不重复插入，避免告警风暴

### 5. Kafka 数据流转
| Topic | 内容 | 消费方（规划） |
|---|---|---|
| `iot.device.data` | 每条遥测属性的完整 JSON | 实时分析、大屏 |
| `iot.alert.triggered` | 告警记录 JSON | AI 诊断 Agent（阶段二） |

### 6. REST 查询接口

```
GET /api/device/{deviceKey}/latest
GET /api/device/{deviceKey}/history?identifier=temperature&limit=50
GET /api/alert/recent?limit=20
```

统一响应格式：
```json
{ "code": 0, "msg": "success", "data": [...] }
```

---

## 快速开始

### 前置条件

以下中间件须已用 Docker 启动：

```bash
# EMQX (MQTT)
docker run -d --name spark-emqx -p 1883:1883 emqx/emqx

# PostgreSQL
docker run -d --name spark-postgres \
  -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root123456 \
  -e POSTGRES_DB=spark_ai -p 5432:5432 postgres:16

# Redis（设备心跳 + 键空间通知）
docker run -d --name spark-redis -p 6379:6379 redis:7

# Kafka (KRaft 模式)
docker run -d --name spark-kafka \
  -p 29092:9092 apache/kafka:latest
```

> **Redis 键空间通知**：本服务启动时自动执行 `CONFIG SET notify-keyspace-events Ex`。
> 如 Redis 实例禁用了 CONFIG SET（如云服务 ACL 限制），需提前手动执行：
> ```bash
> redis-cli CONFIG SET notify-keyspace-events Ex
> ```

> 数据库表（`aiot_device`、`aiot_device_data`、`aiot_alert_rule`、`aiot_alert_record`）
> 由配套的管理系统（spark-iot-agent）负责建表，本服务以 `ddl-auto: none` 直接使用。

### 配置

编辑 `src/main/resources/application.yaml`，默认值已适配本地 Docker 环境：

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/spark_ai
    username: root
    password: root123456
  kafka:
    bootstrap-servers: localhost:29092   # 注意：宿主机映射端口 29092

spring:
  data:
    redis:
      host: localhost
      port: 6379

mqtt:
  host: localhost
  port: 1883

app:
  alert-debounce-minutes: 5            # 同一规则防抖窗口
  device-heartbeat-ttl-seconds: 30     # Redis key TTL，约设备上报周期的 3 倍
  device-heartbeat-key-prefix: "device:online:"
```

### 启动

```bash
# 编译并运行（默认端口 8080）
./gradlew bootRun

# 若 8080 已被占用
./gradlew bootRun --args='--server.port=8081'
```

启动后观察日志中的关键行：
```
[Redis] Keyspace expiry notifications enabled (notify-keyspace-events=Ex)
[MQTT] Connecting to localhost:1883
[MQTT] Subscribed: [GRANTED_QOS_1]
HikariPool-1 - Start completed.
Started SparkAgentEngineApplication in X.XXX seconds
```

### 验收检查

```bash
# 1. 遥测数据持续写入
docker exec <pg-container> psql -U root -d spark_ai \
  -c "SELECT device_key, identifier, value, report_time FROM aiot_device_data ORDER BY create_time DESC LIMIT 10;"

# 2. 设备在线状态（Redis 心跳 key + 数据库）
redis-cli KEYS 'device:online:*'
redis-cli TTL 'device:online:DK_INJ_001'
docker exec <pg-container> psql -U root -d spark_ai \
  -c "SELECT device_key, online_status, last_online_time, last_offline_time FROM aiot_device;"

# 3. 告警记录（需模拟器注入故障，如 temperature > 260）
docker exec <pg-container> psql -U root -d spark_ai \
  -c "SELECT device_key, identifier, trigger_value, alert_content FROM aiot_alert_record ORDER BY trigger_time DESC;"

# 4. Kafka topic 消息数
docker exec spark-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic iot.device.data
docker exec spark-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic iot.alert.triggered

# 5. REST 接口
curl http://localhost:8081/api/device/DK_INJ_001/latest
curl "http://localhost:8081/api/device/DK_INJ_001/history?identifier=temperature&limit=5"
curl http://localhost:8081/api/alert/recent
```

---

## 数据库表说明

本服务操作以下四张表（由管理系统建表，本服务读写）：

| 表名 | 本服务操作 | 说明 |
|---|---|---|
| `aiot_device` | 读取设备信息；更新 `online_status` / `last_online_time` | 设备注册由管理系统维护 |
| `aiot_device_data` | 写入遥测属性行 | 每条 MQTT 消息拆分为多行 |
| `aiot_alert_rule` | 只读，加载启用的告警规则 | 规则由管理系统配置 |
| `aiot_alert_record` | 写入触发的告警记录 | `diagnosis_status=0` 待 AI 诊断 |

所有表 ID 为 `bigint`，不使用数据库自增，由本服务雪花算法生成。

---

## 项目结构

```
src/main/java/com/spark/agent/
├── SparkAgentEngineApplication.java   # 入口，@EnableScheduling
├── common/
│   ├── R.java                         # 统一 API 响应 {code, msg, data}
│   └── SnowflakeIdGenerator.java      # 雪花 ID 生成器（@Component）
├── config/
│   ├── MqttProperties.java            # mqtt.* 配置绑定
│   ├── AppProperties.java             # app.* 配置绑定
│   └── RedisKeyExpirationConfig.java  # RedisMessageListenerContainer + 自动开启键空间通知
├── mqtt/
│   ├── MqttSubscriber.java            # HiveMQ 异步客户端，ApplicationRunner
│   └── DeviceTelemetryMessage.java    # MQTT 消息体 DTO
├── service/
│   ├── TelemetryService.java          # 核心管道：解析→入库→告警→Kafka
│   ├── AlertService.java              # 规则匹配、防抖、写告警记录
│   └── DeviceHeartbeatService.java    # Redis 心跳 + 键空间通知回调，状态变更才写库
├── kafka/
│   └── KafkaProducerService.java      # 封装 KafkaTemplate 发送
├── entity/
│   ├── BaseEntity.java                # ruoyi 公共字段（creator/deleted/tenant_id…）
│   ├── Device.java
│   ├── DeviceData.java
│   ├── AlertRule.java
│   └── AlertRecord.java
├── repository/
│   ├── DeviceRepository.java          # 含 markOnline / markOffline JPQL
│   ├── DeviceDataRepository.java      # 含 findLatestByDeviceKey 关联子查询
│   ├── AlertRuleRepository.java       # findActiveRules (device+product scope)
│   └── AlertRecordRepository.java     # countRecentUnhandled (防抖查询)
└── controller/
    └── ApiController.java             # 3 个 GET 接口
```

---

## Spring Boot 4 兼容性说明

> 本项目在 Spring Boot **4.1.0**（Spring Framework 7 + Hibernate 7 + Jackson 3）上开发，
> 与 Boot 3.x 有若干关键差异：

| 变化 | Boot 3.x | Boot 4.x（本项目） |
|---|---|---|
| Jackson 包名 | `com.fasterxml.jackson.databind` | `tools.jackson.databind` |
| Kafka 自动配置模块 | 内置于 `spring-boot-autoconfigure` | 独立模块 `spring-boot-kafka`，需显式声明 |
| Hibernate | 6.x | 7.4.x |
| Kafka 版本 | 3.x | 4.2.x |

注解包（`@JsonProperty` 等）仍在 `com.fasterxml.jackson.annotation`，未变。

---

## 后续规划

- **阶段二**：接入 Spring AI，消费 `iot.alert.triggered`，对告警记录执行 AI 根因分析，回填 `root_cause` / `suggestion` / `confidence` 字段（表结构已预留）
- **阶段三**：RAG 知识库（设备手册、历史故障案例），增强诊断准确率
- **阶段四**：MCP 接口，供外部 AI Agent 调用设备查询 / 告警管理能力
