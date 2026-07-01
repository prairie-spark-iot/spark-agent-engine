# spark-agent-engine 全面审计报告

**日期**: 2026-07-01
**扫描范围**: 41 个源文件，涵盖 MQTT、Telemetry、Kafka、REST/MCP、RAG/Knowledge、Build/Infrastructure 6 层
**方法**: 6 路并行探索 agent + 直接人工代码审查

---

## 严重性统计

| 严重性 | 数量 | 说明 |
|--------|------|------|
| 🔴 HIGH | 20 | 数据丢失、静默失败、关键链路中断、NPE |
| 🟡 MEDIUM | 35 | 竞态条件、性能问题、监控缺口、正确性 |
| 🔵 LOW | 25 | 代码异味、格式问题、次要问题 |

---

## 🔴 HIGH 严重性（20 项）

### H1. Redis 密码不匹配 — 心跳/离线全线失效

| 字段 | 值 |
|------|-----|
| **文件** | `application.yaml:45` |
| **问题** | `spring.data.redis.password: redis123456`，但 Docker 实例没有 `--requirepass`。Redis 连接失败，错误被 WARN 级别吞没 |
| **影响** | 设备心跳 + 离线检测完全静默失效 |
| **修复** | 要么从 yaml 移除密码，要么 Docker 启动时加 `redis-server --requirepass redis123456` |

### H2. `MqttSubscriber` 无 `@PreDestroy` — 连接泄漏

| 字段 | 值 |
|------|-----|
| **文件** | `MqttSubscriber.java:34-47` |
| **问题** | 没有 `@PreDestroy` 方法，应用关闭时 HiveMQ 客户端不调用 `client.disconnect()` |
| **影响** | Netty 线程 + TCP 连接泄漏，K8s 滚动重启时阻止 Pod 优雅终止 |
| **修复** | 添加 `@PreDestroy public void destroy() { client.disconnect(); }` |

### H3. Netty 事件循环被 DB I/O 阻塞

| 字段 | 值 |
|------|-----|
| **文件** | `MqttSubscriber.java:71-79` |
| **问题** | HiveMQ 回调直接在 Netty 事件循环线程执行，但 `handleMessage()` 调用 `TelemetryService.process()` 做阻塞 DB I/O |
| **影响** | 阻塞心跳 PING → EMQX 超时断连 |
| **修复** | 用虚拟线程 executor 异步执行 |

### H4. `AlertTriggeredConsumer` 解析失败后无 `return`

| 字段 | 值 |
|------|-----|
| **文件** | `AlertTriggeredConsumer.java:21-31` |
| **问题** | `readValue()` 抛异常后 catch 仅打日志，无 `return`，继续执行 `diagnose(payload)` 传入原始字符串 |
| **影响** | 损坏的消息引发二次解析异常，LLM 浪费 + 诊断记录损坏 |
| **修复** | catch 块中加 `return;` |

### H5. Kafka fire-and-forget 无错误传递

| 字段 | 值 |
|------|-----|
| **文件** | `KafkaProducerService.java:37-42` |
| **问题** | `CompletableFuture` 不 join。发送失败仅日志，调用方（`@Transactional`）正常返回。DB 提交但 Kafka 丢消息 |
| **影响** | 数据不一致：DB 有数据但下游 Kafka 消费者无感知 |
| **修复** | 抛出异常触发事务回滚（at-most-once），或者实现 transactional outbox |

### H6. `@Transactional` 内 Kafka 发消息

| 字段 | 值 |
|------|-----|
| **文件** | `TelemetryService.java:51-52` |
| **问题** | `TelemetryService.process()` 是 `@Transactional`，Kafka 发送在事务内回滚风险 |
| **影响** | 事务回滚后 Kafka 消息已发出（幽灵消息） |
| **状态** | 已知设计限制，阶段二改 transactional outbox |

### H7. `KnowledgeIngestionService.ingest()` 无 `@Transactional`

| 字段 | 值 |
|------|-----|
| **文件** | `KnowledgeIngestionService.java:31` |
| **问题** | 每个 chunk 先 `knowledgeRepository.save()` 再 `vectorStoreRepository.saveEmbedding()`。后面步骤失败时前面已提交 |
| **影响** | 孤立 `knowledge` 行无对应 embedding |
| **修复** | 加 `@Transactional` |

### H8. `VectorStoreRepository.saveEmbedding()` 是 UPDATE

| 字段 | 值 |
|------|-----|
| **文件** | `VectorStoreRepository.java:22-25` |
| **问题** | `UPDATE aiot_knowledge SET embedding = ? WHERE id = ?`。如果该行不存在（因前面事务回滚或删除了），执行成功但 0 行受影响 |
| **影响** | 静默数据丢失 |
| **修复** | 改为 `INSERT INTO aiot_knowledge (...) VALUES (...) ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding` |

### H9. `TelemetryService.buildRows()` NPE

| 字段 | 值 |
|------|-----|
| **文件** | `TelemetryService.java:61` |
| **问题** | `msg.getProperties().entrySet()` — `getProperties()` 可能为 null |
| **影响** | NPE 导致整个事务回滚，数据行丢失 |
| **修复** | 加空判断 |

### H10. `DiagnosisAgentService.runInference()` 无超时

| 字段 | 值 |
|------|-----|
| **文件** | `DiagnosisAgentService.java:137-143` |
| **问题** | `chatClient.prompt().call()` 没有 `.timeout()` |
| **影响** | Ollama 卡住时 Kafka 监听器线程被永久阻塞，耗尽线程池 |
| **修复** | 加 `.timeout(Duration.ofSeconds(60))` |

### H11. `SnowflakeIdGenerator.machineId` 硬编码 1

| 字段 | 值 |
|------|-----|
| **文件** | `SnowflakeIdGenerator.java:22` |
| **问题** | `this.machineId = 1`，没有配置注入 |
| **影响** | 第二个实例产生 ID 碰撞 → 主键冲突 → 插入失败 |
| **修复** | 从环境变量或配置读取 |

### H12. Snowflake 时钟回滚导致序列重置

| 字段 | 值 |
|------|-----|
| **文件** | `SnowflakeIdGenerator.java:35-36` |
| **问题** | 时钟回滚后 `lastTimestamp > ts`，`ts == lastTimestamp` 不成立，`sequence` 重置为 0 |
| **影响** | 重复 ID |
| **修复** | 加时钟回滚检测，`if (ts < lastTimestamp)` 抛异常或等待 |

### H13. `AlertService.matches()` 浮点 `eq`/`ne` 不可靠

| 字段 | 值 |
|------|-----|
| **文件** | `AlertService.java:54-55` |
| **问题** | `value == threshold` 对 235.5 这样的浮点值可能不可靠 |
| **影响** | 精确等于/不等于的比较可能错误 |
| **修复** | `eq`/`ne` 计算 epsilon 容差 |

### H14. `AlertService` N+1 告警规则查询

| 字段 | 值 |
|------|-----|
| **文件** | `AlertService.java:32` |
| **问题** | 每个设备属性行都执行 `findActiveRules(deviceId, identifier)` |
| **影响** | 同设备 40+ 属性的消息产生 40+ SELECT |
| **修复** | 缓存规则或批量查询 |

### H15. `DiagnosisAgentService.writeBack()` 强制 `deleted=0`

| 字段 | 值 |
|------|-----|
| **文件** | `DiagnosisAgentService.java:157` |
| **问题** | `record.setDeleted((short) 0)` 无先校验原值 |
| **影响** | 可以撤销软删除的记录 |
| **修复** | 不应强制设置 `deleted` |

### H16. `DeviceHeartbeatService` TOCTOU 离线竞态

| 字段 | 值 |
|------|-----|
| **文件** | `DeviceHeartbeatService.java:51-63` |
| **问题** | Redis key 过期到 `onMessage()` 执行之间设备可能发送新心跳。无条件写 `online_status=0` |
| **影响** | 活跃设备被错误标记为离线 |
| **修复** | 执行 `markOffline()` 前检查 key 是否存在 |

### H17. 无 `spring-boot-starter-validation`

| 字段 | 值 |
|------|-----|
| **文件** | `build.gradle` |
| **问题** | `@Valid` / `@NotBlank` 等注解静默失效 |
| **影响** | 所有 API 参数无校验 |
| **修复** | 加依赖 + 加注解 |

### H18. 无 `@RestControllerAdvice`

| 字段 | 值 |
|------|-----|
| **文件** | 整个项目 |
| **问题** | 无全局异常处理器 |
| **影响** | 所有异常输出 Spring 默认错误格式，"R.fail()" 是死代码 |

### H19. 无身份认证/授权

| 字段 | 值 |
|------|-----|
| **文件** | 整个项目 |
| **问题** | 无 Spring Security，无 API Key，无 CORS |
| **影响** | 任何网络端点可调用所有接口 |

### H20. API/MCP 返回 JPA 实体

| 字段 | 值 |
|------|-----|
| **文件** | `ApiController.java`, `DeviceMcpToolService.java` |
| **问题** | 直接返回 `DeviceData`, `AlertRecord` 等实体对象 |
| **影响** | 暴露 `deleted`, `tenantId`, `updater` 等内部字段 |

---

## 🟡 MEDIUM 严重性（35 项 — 摘要）

| # | 文件:行 | 问题 | 影响 |
|---|---------|------|------|
| M1 | `AlertService.java:67-69` | 防抖检查-插入非原子 | 重复告警 |
| M2 | `KafkaConsumerConfig.java` | DLT 绕过致命异常（如反序列化失败依然重试 2 次） | 中毒消息反复重试 |
| M3 | 整个项目 | Kafka 消费者配置不完整 | 潜在重平衡问题 |
| M4 | `KnowledgeIngestionService.java:72-102` | chunk() 无重叠，500 字硬切 | 语义断裂 |
| M5 | `KnowledgeIngestionService.java:53-70` | `importBatch()` 无 `@Transactional` | 批量部分提交 |
| M6 | `DiagnosisAgentService.java:146` | `needsReflection()` 在空 RAG 结果时触发 | 浪费 LLM 调用 |
| M7 | `RagSearchService.java:18` | `embed()` 无错误处理 | LLM 异常杀死 Kafka 消费者 |
| M8 | `DiagnosisAgentService.java:173` | `formatManuals()` 无界摘录 | LLM 上下文窗口溢出 |
| M9 | 整个项目 | 批量导入无大小限制 | OOM / 资源耗尽 |
| M10 | `Application.yaml:45` | Redis 密码明文 | 安全风险 |
| M11 | `DeviceHeartbeatService.java:41` | `setIfAbsent` + `expire` 非原子 | 极短窗口竞态 |
| M12 | `DeviceRepository.java:15-21` | `@Modifying` 缺少 `flushAutomatically` / `clearAutomatically` | 脏读 |
| M13 | `RedisKeyExpirationConfig.java:19-27` | 无独立线程池 | 过期消息洪峰阻塞 |
| M14 | `TelemetryService.java:51` | `saveAll()` 后未 flush 就 Kafka 发送 | 顺序竞争 |
| M15 | `VectorStoreRepository.java:49-56` | SQL 用 `String.formatted()` 拼接 | SQL 注入隐患 |
| M16 | `ApiController.java` | limit 参数无上限 | 内存溢出 |
| M17 | `ApiController.java:24-26` | 设备不存在返回 200 而非 404 | API 误用 |
| M18 | `R.java` | `fail()` 永远返回 code 500 | 无法区分错误类型 |
| M19 | `RagController.java:34` | `SearchRequest.topK` 可能 <= 0 | 语义异常 |
| M20 | `MqttSubscriber.java:57` | 首次连接失败 fall-through | 静默失败 |

剩余 15 项 MEDIUM 问题详见各 agent 输出文件。

---

## 🔵 LOW 严重性（25 项 — 摘要）

包括：日志拼写错误、变量命名不一致、死代码（`AlertRecordRepository.findTop5` 未使用）、`BaseEntity.@PrePersist` 在 JDBC 路径下无效、格式缩进不一致、文档与实际行为不符等。

---

## 设计说明（已知限制，非 bug）

1. **Kafka at-most-once** — 在 `@Transactional` 内发 Kafka，不回滚 Kafka 消息。阶段二用 transactional outbox
2. **无 `@EnableKafka`** — 依赖 `spring-boot-kafka` 自动配置。如监听器不触发可以显式加
3. **无 schema 迁移** — `ddl-auto: none`，表由外部系统管理
4. **无 rate limiting** — 所有端点无保护，适合阶段二加

---

## 附件

审计引用来源：
- MQTT 层深度审查 → `bg_54e803c9`
- Telemetry 管道审查 → `bg_88700db2`
- Kafka 层审查 → `bg_bf0e59a9`
- REST/MCP API 审查 → `bg_8b23faa1`
- RAG/Knowledge 审查 → `bg_4850af1d`
- 构建/基础设施审查 → `bg_4bb4fbd0`
