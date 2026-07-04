# spark-agent-engine

**工业 IoT 设备 AI 诊断引擎** —— 从 MQTT 遥测接入到告警触发、再到 LLM 根因诊断的全链路自动化闭环。

> Industrial IoT telemetry ingestion, alerting, and autonomous AI root-cause diagnosis — one Spring Boot 4 / Java 25 service, zero manual triage.

---

## 项目背景 · Background

工业设备的异常诊断长期依赖人工经验：设备离线/超温/超压等异常发生后，运维人员需要手动翻查历史数据、比对设备手册、判断根因，响应慢且高度依赖个人经验。

**spark-agent-engine** 把这条链路完全自动化：设备数据经 MQTT 实时接入 → 落库 + 规则引擎判断异常 → 告警自动触发 → LLM Agent 自主调用工具（查询设备状态、历史遥测、告警记录、设备手册）完成根因分析，并把结论回写数据库供人工复核。整个过程无需人工介入即可产出"可信度评分 + 根因 + 处置建议"。

本仓库是生态中的**数据与 AI 诊断核心引擎**；配套的管理后台（RBAC、设备/规则配置界面）由姊妹项目 [spark-iot-agent](../spark-iot-agent) 提供，两者共享同一套 PostgreSQL 表结构。

---

## 系统架构 · Architecture

```
                        ┌──────────────────────┐
  设备模拟器 ──MQTT──►  │   EMQX (MQTT 5.0)     │
  (spark-iot-emulator)  └──────────┬───────────┘
                                   │ QoS 1, 断线自动重连
                                   ▼
                     ┌─────────────────────────────┐
                     │      spark-agent-engine       │
                     │                               │
                     │  MqttSubscriber (HiveMQ 异步)  │
                     │        │                      │
                     │        ▼                      │
                     │  TelemetryService (@Tx)        │
                     │   ├─ DeviceHeartbeatService ──────► Redis (SETNX+EX / 键空间通知)
                     │   ├─ DeviceDataRepository       │
                     │   ├─ AlertService (规则引擎+防抖) │
                     │   └─ OutboxMessage (同事务写入)  │
                     │        │                      │
                     │        ▼                      │
                     │  OutboxRelayService (@Scheduled) │
                     │        │  at-least-once         │
                     │        ▼                      │
                     │      Kafka ──────────────────────► iot.device.data
                     │        │                        │  iot.alert.triggered
                     │        ▼                      │
                     │  AlertTriggeredConsumer          │
                     │        │                      │
                     │        ▼                      │
                     │  DiagnosisAgentService           │
                     │   ├─ Spring AI ChatClient ────────► Ollama (qwen2.5:7b)
                     │   ├─ MCP Tool Callbacks           │   ├─ listDevices / queryDeviceStatus
                     │   │   (DeviceMcpToolService)       │   ├─ queryDeviceHistory / queryDeviceAlerts
                     │   │   自主决定调用哪些工具          │   └─ queryDeviceManual (RAG)
                     │   └─ 低置信度 → 扩窗重试一次        │
                     │        │                      │
                     │        ▼                      │
                     │  写回 aiot_alert_record          │
                     │   (root_cause/suggestion/       │
                     │    confidence/diagnosis_status) │
                     │                               │
                     │  RagSearchService ◄── pgvector ──► aiot_knowledge (HNSW, 768维)
                     │  ApiController (REST GET)        │
                     └─────────────────────────────┘
                                   ▲
                     PostgreSQL 表结构由 spark-iot-agent（RBAC 管理后台）维护
```

---

## 核心功能 · Core Features

| 模块 | 说明 |
|---|---|
| **遥测接入** | HiveMQ 异步客户端订阅 MQTT 5，断线自动重连+重订阅；单条消息异常隔离，不影响后续消息 |
| **设备在线状态** | Redis `SETNX+EX` 心跳 + 键空间通知事件驱动下线检测，状态不变时零数据库写入 |
| **告警规则引擎** | 6 种比较运算符（`gt/lt/gte/lte/eq/ne`，类型安全枚举），支持设备级/全局规则，`pg_advisory_xact_lock` 保证跨实例防抖不重复告警 |
| **事务性 Outbox** | 遥测与告警写入与 Outbox 记录同事务提交，`OutboxRelayService` 定时轮询转发 Kafka，实现 at-least-once，杜绝"DB 回滚但 Kafka 已发出"的幽灵数据问题 |
| **AI 根因诊断** | `DiagnosisAgentService` 基于 Spring AI + Ollama，LLM 自主调用工具收集上下文后给出结构化诊断（根因/建议/置信度），低置信度自动扩大时间窗口重试一次 |
| **MCP 工具服务** | `DeviceMcpToolService` 暴露 5 个 `@Tool` 方法（设备列表/状态/历史/告警/手册检索），供 LLM Agent 及外部 MCP 客户端调用 |
| **RAG 知识库** | 设备手册/SOP/故障案例经 Ollama `nomic-embed-text` 生成 768 维向量，写入 pgvector，`<->` 余弦距离检索为诊断提供上下文 |
| **REST 查询接口** | 设备最新值/历史/告警记录只读接口，统一 `{code,msg,data}` 响应，DTO 层与 JPA 实体解耦 |

---

## 技术栈 · Tech Stack

| 分类 | 技术 |
|---|---|
| **后端** | Java 25 · Spring Boot 4.1.0 · Spring Framework 7 · Hibernate 7.4 · Gradle 9.5 |
| **数据库** | PostgreSQL（`pgvector` 扩展，HNSW 向量索引）· Spring Data JPA |
| **AI / LLM** | Spring AI 2.0 · Ollama（`qwen2.5:7b` 推理 / `nomic-embed-text` 向量化）· MCP Server（STREAMABLE 协议） |
| **中间件** | EMQX（MQTT 5.0）· Apache Kafka 4.x（Spring for Apache Kafka）· Redis 7（心跳 + 键空间通知） |
| **消息可靠性** | 事务性 Outbox 模式 + 定时中继（at-least-once） |
| **其他** | HiveMQ MQTT Client 1.3.15（异步 API）· Jackson 3.x（`tools.jackson.*`）· Lombok · Snowflake ID |

---

## 数据库设计亮点 · Schema Highlights

本服务读写 `aiot_*` 系列表，Schema 由姊妹项目 `spark-iot-agent`（RBAC 管理后台）统一维护，本服务 `ddl-auto: none` 只读写不建表：

| 表 | 关键设计 |
|---|---|
| `aiot_device` / `aiot_product` | 设备-产品关联，`online_status` 状态机仅在离线↔在线转换时写库 |
| `aiot_device_data` | 遥测明细表，`value`（文本）+ `value_num`（`numeric(20,4)`）双列存储；`(device_key, identifier, report_time DESC) WHERE deleted=0` 复合索引把 80k 行全表扫描从 68s 降到 ~200ms；`findLatestByDeviceKey` 用 `ROW_NUMBER() OVER (PARTITION BY identifier)` 窗口函数替代关联子查询 |
| `aiot_alert_rule` / `aiot_alert_record` | 规则 `threshold` 为 VARCHAR 运行时解析；告警防抖索引 `(device_id, rule_id, trigger_time DESC) WHERE handle_status=0` 支撑高频热路径查询；`diagnosis_status/root_cause/suggestion/confidence` 字段为 AI 诊断回写预留 |
| `aiot_knowledge` | **RAG 向量表**：`embedding vector(768)` 存储 `nomic-embed-text` 生成的语义向量，`USING hnsw (embedding vector_cosine_ops)` 索引支撑近似最近邻检索；`device_model`/`product_id` 双维度过滤实现"先按设备型号缩小范围、再语义检索"的组合策略 |
| `aiot_outbox`（本服务自有） | 事务性 Outbox 落地表，`(created_at) WHERE published_at IS NULL` 部分索引让"待发布消息"查询免扫描全表；每日定时清理已发布的过期行 |

所有主键均为 `bigint`，由内置雪花算法生成（41 位时间戳 | 10 位机器号 | 12 位序列），不依赖数据库自增。

---

## 快速开始 · Quick Start

```bash
# 1. 启动中间件（EMQX / PostgreSQL(pgvector) / Redis / Kafka，见姊妹仓库 spark-ai-infra）
cd ../spark-ai-infra && docker compose up -d

# 2. 本机启动 Ollama 并拉取模型（推理 + 向量化）
ollama pull qwen2.5:7b
ollama pull nomic-embed-text

# 3. 编译运行本服务（默认端口 8080）
./gradlew bootRun

# 4. 验证
curl http://localhost:8080/api/device/DK_INJ_001/latest
curl http://localhost:8080/api/alert/recent
```

> 生产/容器化部署可用本仓库的 `docker-compose.yml` 单独构建运行 app 容器（依赖上述中间件的外部网络 `spark-ai-infra_spark-net`）。

详细的表结构初始化、Redis 键空间通知配置、单测运行等说明见 [`AGENTS.md`](./AGENTS.md) / [`CLAUDE.md`](./CLAUDE.md)。

---

## 项目截图 / 演示 · Screenshots & Demo

> TODO: 添加截图（告警列表、AI 诊断结果展示、RAG 检索命中示例）

---

## 相关项目 · Related Projects

- [spark-iot-agent](../spark-iot-agent) —— 基于 ruoyi-vue-pro（yudao）二次开发的管理后台，提供设备/产品/告警规则的 RBAC 管理界面，与本服务共享 `aiot_*` 表结构
- [spark-iot-emulator](../spark-iot-emulator) —— 设备遥测模拟器，向 EMQX 发布 MQTT 遥测消息，用于本地开发与故障场景复现

---

## 许可证 · License

本项目基于 [MIT License](./LICENSE) 开源。

---

## 技术亮点 · Highlights

- **事务性 Outbox 消除幽灵数据**：遥测/告警写入与 Outbox 记录同一事务提交，定时中继转发 Kafka，把原本"DB 回滚但消息已发出"的 at-most-once 缺陷升级为 at-least-once，用一张表 + 一个 `@Scheduled` 任务解决了分布式系统里最常见的数据一致性陷阱。
- **LLM 自主工具调用做根因诊断**：`DiagnosisAgentService` 不是简单地把遥测塞进 Prompt，而是通过 Spring AI 把 5 个 MCP 工具（设备状态/历史/告警/手册检索）交给 LLM 自主决策调用，诊断结果低于 80% 置信度时自动扩大 120 分钟历史窗口重试一次。
- **pgvector + HNSW 实现毫秒级语义检索**：设备手册/故障案例经 `nomic-embed-text` 编码为 768 维向量存入 PostgreSQL，`HNSW` 近似最近邻索引配合 `device_model` 元数据过滤，替代传统关键词全文检索，让 RAG 检索延迟稳定在毫秒级。
- **数据库级分布式锁保证告警防抖零重复**：告警去重从最初的 JVM 内 `synchronized` 升级为 `pg_advisory_xact_lock`，跨实例、跨线程的"同设备同规则并发触发"场景下经并发测试验证不产生重复告警。
- **116 个单元测试覆盖热路径**：遥测入库、告警规则 6 种运算符全覆盖、Outbox 中继、诊断置信度分支、MCP 工具映射均有独立测试，核心链路变更可在秒级本地验证而非依赖联调。
