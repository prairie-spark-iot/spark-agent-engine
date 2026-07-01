# Knowledge Batch Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add `POST /api/knowledge/import` — a batch endpoint that embeds pre-chunked JSON knowledge entries via Ollama `nomic-embed-text` and inserts them into `aiot_knowledge` via raw JdbcTemplate SQL.

**Architecture:** New `KnowledgeController` (`/api/knowledge`) → new `KnowledgeIngestionService.importBatch(...)` method (per-item try/catch loop, reusing the service's existing `EmbeddingModel`/`SnowflakeIdGenerator` wiring) → new `VectorStoreRepository.insertKnowledge(...)` method (single raw `INSERT` with `::vector` cast, reusing the existing `toVectorString(float[])` helper). Two new DTOs (`KnowledgeImportItem`, `KnowledgeImportResult`) carry the request/response shape.

**Tech Stack:** Spring Boot 4.1.0, Java 25, Spring AI 2.0.0 (`EmbeddingModel.embed(String) → float[]`, auto-configured by `spring-ai-starter-model-ollama`), Spring Data JPA (unused for this path — raw JDBC only), `JdbcTemplate`, PostgreSQL `pgvector`.

## Global Constraints

- Package root: `com.spark.agent`. Follow the existing flat-by-layer package convention (`controller`, `service`, `repository`, `dto`, `common`) — no feature subpackages.
- Use `tools.jackson.*` semantics are irrelevant here (no custom Jackson config needed — default Spring Boot 4 / Jackson 3 record (de)serialization handles `List<KnowledgeImportItem>` from a raw JSON array body).
- `EmbeddingModel` is already an auto-configured Spring bean (`spring.ai.model.embedding: ollama`, `spring.ai.ollama.embedding.model: nomic-embed-text` in `application.yaml`) — do not add a new `@Configuration` class for it.
- `aiot_knowledge.doc_type` is `smallint NOT NULL` — a missing/null `docType` in a JSON item must surface as a per-item failure (DB constraint violation caught by the per-item try/catch), not a validation framework.
- `aiot_knowledge.product_id` and `device_model` are nullable — pass through as-is, no null-checks needed.
- **No unit test suite exists for services in this codebase** (e.g. `KnowledgeIngestionService.ingest()` has zero test coverage; the only test file is the default `SparkAgentEngineApplicationTests` context-load test). This plan follows that established convention: verify via `./gradlew build -x test` (compile check) plus a manual integration test (`curl` + `psql`) against the running app, rather than introducing JUnit tests that don't match the codebase's actual practice.
- Full spec: `docs/superpowers/specs/2026-06-30-knowledge-batch-import-design.md`.

---

### Task 1: Add `KnowledgeImportItem` and `KnowledgeImportResult` DTOs

**Files:**
- Create: `src/main/java/com/spark/agent/dto/KnowledgeImportItem.java`
- Create: `src/main/java/com/spark/agent/dto/KnowledgeImportResult.java`

**Interfaces:**
- Produces: `KnowledgeImportItem(String title, Long productId, String deviceModel, Short docType, String source, String chunkText)` — used by Task 2 (`VectorStoreRepository.insertKnowledge`), Task 3 (`KnowledgeIngestionService.importBatch`), Task 4 (`KnowledgeController.importBatch`).
- Produces: `KnowledgeImportResult(int successCount, int failCount, List<KnowledgeImportResult.FailedItem> failedItems)` with nested `record FailedItem(String title, String reason)` — used by Task 3 and Task 4.

- [x] **Step 1: Write `KnowledgeImportItem.java`**

```java
package com.spark.agent.dto;

public record KnowledgeImportItem(String title, Long productId, String deviceModel,
                                   Short docType, String source, String chunkText) {
}
```

- [x] **Step 2: Write `KnowledgeImportResult.java`**

```java
package com.spark.agent.dto;

import java.util.List;

public record KnowledgeImportResult(int successCount, int failCount, List<KnowledgeImportResult.FailedItem> failedItems) {
    public record FailedItem(String title, String reason) {
    }
}
```

- [x] **Step 3: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [x] **Step 4: Commit**

```bash
git add src/main/java/com/spark/agent/dto/KnowledgeImportItem.java src/main/java/com/spark/agent/dto/KnowledgeImportResult.java
git commit -m "feat(knowledge): add batch import DTOs"
```

---

### Task 2: Add `VectorStoreRepository.insertKnowledge(...)`

**Files:**
- Modify: `src/main/java/com/spark/agent/repository/VectorStoreRepository.java`

**Interfaces:**
- Consumes: `KnowledgeImportItem` (Task 1), private `toVectorString(float[])` helper already in this class (line 57-64 of the existing file).
- Produces: `public void insertKnowledge(Long id, KnowledgeImportItem item, float[] embedding)` — used by Task 3 (`KnowledgeIngestionService.importBatch`).

- [x] **Step 1: Add the import and method**

Add this import near the top of `VectorStoreRepository.java` (alongside the existing imports):

```java
import com.spark.agent.dto.KnowledgeImportItem;
```

Add this method to the class, after `saveEmbedding(...)` and before `search(...)`:

```java
    public void insertKnowledge(Long id, KnowledgeImportItem item, float[] embedding) {
        jdbc.update("""
                INSERT INTO aiot_knowledge (id, title, doc_type, product_id, device_model,
                  chunk_text, embedding, source, creator, tenant_id, deleted, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?::vector, ?, 'system', 1, 0, now(), now())
                """,
                id, item.title(), item.docType(), item.productId(), item.deviceModel(),
                item.chunkText(), toVectorString(embedding), item.source());
    }
```

- [x] **Step 2: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/spark/agent/repository/VectorStoreRepository.java
git commit -m "feat(knowledge): add raw-SQL insertKnowledge to VectorStoreRepository"
```

---

### Task 3: Add `KnowledgeIngestionService.importBatch(...)`

**Files:**
- Modify: `src/main/java/com/spark/agent/service/KnowledgeIngestionService.java`

**Interfaces:**
- Consumes: `KnowledgeImportItem`, `KnowledgeImportResult`, `KnowledgeImportResult.FailedItem` (Task 1); `VectorStoreRepository.insertKnowledge(Long, KnowledgeImportItem, float[])` (Task 2); existing fields `embeddingModel` (`EmbeddingModel.embed(String) → float[]`), `idGen` (`SnowflakeIdGenerator.nextId() → Long`), both already present in this class.
- Produces: `public KnowledgeImportResult importBatch(List<KnowledgeImportItem> items)` — used by Task 4 (`KnowledgeController`).

- [x] **Step 1: Add imports and the method**

Add these imports near the top of `KnowledgeIngestionService.java` (alongside the existing imports):

```java
import com.spark.agent.dto.KnowledgeImportItem;
import com.spark.agent.dto.KnowledgeImportResult;
import com.spark.agent.dto.KnowledgeImportResult.FailedItem;
```

Add this method to the class, after `ingest(...)` and before the private `chunk(...)` method:

```java
    public KnowledgeImportResult importBatch(List<KnowledgeImportItem> items) {
        int successCount = 0;
        List<FailedItem> failedItems = new ArrayList<>();

        for (KnowledgeImportItem item : items) {
            try {
                float[] embedding = embeddingModel.embed(item.chunkText());
                vectorStoreRepository.insertKnowledge(idGen.nextId(), item, embedding);
                successCount++;
            } catch (Exception e) {
                log.error("[Knowledge Import] Failed to import '{}': {}", item.title(), e.getMessage(), e);
                failedItems.add(new FailedItem(item.title(), e.getMessage()));
            }
        }

        log.info("[Knowledge Import] {} succeeded, {} failed out of {}", successCount, failedItems.size(), items.size());
        return new KnowledgeImportResult(successCount, failedItems.size(), failedItems);
    }
```

Note: `ArrayList` and `List` are already imported by the existing file (used by `chunk(...)`), so no new collection imports are needed.

- [x] **Step 2: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/spark/agent/service/KnowledgeIngestionService.java
git commit -m "feat(knowledge): add importBatch to KnowledgeIngestionService"
```

---

### Task 4: Add `KnowledgeController` and verify end-to-end

**Files:**
- Create: `src/main/java/com/spark/agent/controller/KnowledgeController.java`

**Interfaces:**
- Consumes: `KnowledgeImportItem`, `KnowledgeImportResult` (Task 1); `KnowledgeIngestionService.importBatch(List<KnowledgeImportItem>)` (Task 3); `com.spark.agent.common.R` (existing response wrapper, `R.ok(T data)`).
- Produces: `POST /api/knowledge/import` HTTP endpoint.

- [x] **Step 1: Write `KnowledgeController.java`**

```java
package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.dto.KnowledgeImportItem;
import com.spark.agent.dto.KnowledgeImportResult;
import com.spark.agent.service.KnowledgeIngestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/knowledge")
@RequiredArgsConstructor
public class KnowledgeController {

    private final KnowledgeIngestionService ingestionService;

    @PostMapping("/import")
    public R<KnowledgeImportResult> importBatch(@RequestBody List<KnowledgeImportItem> items) {
        return R.ok(ingestionService.importBatch(items));
    }
}
```

- [x] **Step 2: Compile**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [x] **Step 3: Confirm infra is up**

Run: `docker ps --format '{{.Names}}'`
Expected: sees running containers for EMQX, PostgreSQL, Kafka, Redis (per CLAUDE.md's infra table). If Ollama is not in Docker, separately confirm it's reachable:
Run: `curl -s http://localhost:11434/api/tags | head -c 200`
Expected: JSON listing local models, including `nomic-embed-text`.

- [x] **Step 4: Start the app**

Run (background): `./gradlew bootRun --args='--server.port=8081'`
Expected: log line `Started SparkAgentEngineApplication` with no stack trace.

- [x] **Step 5: Write a small test fixture and call the endpoint**

Write `/tmp/claude-1000/-home-spark-Projects-AI-spark-agent-engine/d1ef3b3b-8da0-4bbe-97c8-0bc181052659/scratchpad/knowledge-import-test.json`:

```json
[
  {
    "title": "注塑机机筒温度异常升高",
    "productId": 1001,
    "deviceModel": "PK_INJECTION_MA",
    "docType": 3,
    "source": "注塑机故障诊断手册-温度章节",
    "chunkText": "故障现象：机筒温度持续超过设定值上限，可能伴随物料降解、异味。常见原因：加热圈损坏、温控热电偶松动或校准偏差、冷却水路堵塞。处理建议：检查加热圈阻值，校准热电偶，清理冷却水路。"
  },
  {
    "title": "无 productId 的条目",
    "productId": null,
    "deviceModel": "PK_INJECTION_MA",
    "docType": 3,
    "source": "测试",
    "chunkText": "用于验证 productId 为 null 时导入是否正常。"
  }
]
```

Run:
```bash
curl -s -X POST http://localhost:8081/api/knowledge/import \
  -H 'Content-Type: application/json' \
  -d @/tmp/claude-1000/-home-spark-Projects-AI-spark-agent-engine/d1ef3b3b-8da0-4bbe-97c8-0bc181052659/scratchpad/knowledge-import-test.json
```

Expected: `{"code":0,"msg":"success","data":{"successCount":2,"failCount":0,"failedItems":[]}}`

- [x] **Step 6: Verify rows landed in Postgres with a real vector**

Run:
```bash
docker exec spark-postgres psql -U root -d spark_ai -c \
  "SELECT id, title, product_id, device_model, doc_type, vector_dims(embedding) AS dims FROM aiot_knowledge WHERE source IN ('注塑机故障诊断手册-温度章节', '测试') ORDER BY create_time DESC LIMIT 2;"
```

Expected: 2 rows, `dims = 768`, the second row's `product_id` is `NULL`.

- [x] **Step 7: Stop the app**

Stop the background `bootRun` process (e.g. via the background task tool / `kill` the tracked PID).

- [x] **Step 8: Commit**

```bash
git add src/main/java/com/spark/agent/controller/KnowledgeController.java
git commit -m "feat(knowledge): add POST /api/knowledge/import endpoint"
```

---

## Deviation Found During Execution

Task 4 verification (Step 5-6) initially returned `successCount: 2` but 0 rows landed in Postgres. Root cause: `application.yaml` sets `spring.datasource.hikari.auto-commit: false` ("@Transactional owns commit boundaries" per its own comment) project-wide, and the plan's `insertKnowledge` method (Task 2) was missing `@Transactional` — unlike the pre-existing `saveEmbedding` method in the same class, which has it. The insert executed without error but was never committed. Fixed by adding `@Transactional` to `insertKnowledge`; re-verified successfully (rows persisted with correct 768-dim vectors, null `product_id` handled, and a mixed success/failure batch correctly committed the success and rolled back only the failed row).

## Self-Review Notes

- **Spec coverage:** all 5 numbered requirements from the spec (batch iterate, embed via `EmbeddingModel`, insert via raw JdbcTemplate SQL with `::vector`, return success/fail counts + continue-on-failure, exact SQL shape) are covered by Tasks 1-4. `productId` nullable handling is exercised in Task 4 Step 5's second fixture item.
- **Type consistency:** `KnowledgeImportItem` field names/types are identical across Task 1 (definition), Task 2 (`insertKnowledge` signature), Task 3 (`importBatch` loop), and Task 4 (`@RequestBody List<KnowledgeImportItem>`). `KnowledgeImportResult`/`FailedItem` likewise consistent between Task 1, 3, 4.
- **No placeholders:** all steps contain complete, exact code and commands.
