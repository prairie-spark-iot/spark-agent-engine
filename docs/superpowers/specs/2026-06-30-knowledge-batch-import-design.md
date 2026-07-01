# Knowledge Batch Import — Design

## Purpose

Add `POST /api/knowledge/import` to bulk-load pre-chunked knowledge entries (e.g. from `device-knowledge.json`) into `aiot_knowledge`, embedding each entry's `chunkText` via the Ollama `nomic-embed-text` model (768 dims) through Spring AI's `EmbeddingModel`.

This is distinct from the existing `POST /api/rag/ingest` (`RagController` → `KnowledgeIngestionService.ingest(...)`), which takes one long document and paragraph-chunks it into multiple rows. The import endpoint's input is already chunked — one JSON array element maps to exactly one `aiot_knowledge` row, with no re-chunking.

## Request / Response

```
POST /api/knowledge/import
Content-Type: application/json

[
  {
    "title": "注塑机机筒温度异常升高",
    "productId": 1001,
    "deviceModel": "PK_INJECTION_MA",
    "docType": 3,
    "source": "注塑机故障诊断手册-温度章节",
    "chunkText": "故障现象：..."
  },
  ...
]
```

Response: `R<KnowledgeImportResult>` where

```java
record KnowledgeImportResult(int successCount, int failCount, List<FailedItem> failedItems) {
    record FailedItem(String title, String reason) {}
}
```

`failedItems` carries the title + exception message for each failed row, so a bad batch can be diagnosed without grepping logs (still logged too, per existing project convention).

## Components

| File | Change |
|---|---|
| `dto/KnowledgeImportItem.java` | New record: `title, productId, deviceModel, docType, source, chunkText` — 1:1 with JSON fields. `productId` (`Long`) and `deviceModel` (`String`) are nullable, matching the nullable DB columns. |
| `dto/KnowledgeImportResult.java` | New record: `successCount, failCount, failedItems` (see above). |
| `controller/KnowledgeController.java` | New `@RestController` at `/api/knowledge`. `POST /import` accepts `List<KnowledgeImportItem>` as the raw JSON array body (not wrapped in an envelope object), returns `R<KnowledgeImportResult>`. Separate from `RagController` since the path prefix differs (`/api/knowledge` vs `/api/rag`). |
| `service/KnowledgeIngestionService.java` | New method `importBatch(List<KnowledgeImportItem> items)`. Loops per item — embeds via `embeddingModel.embed(String)` and inserts via `vectorStoreRepository.insertKnowledge(...)` inside a try/catch per item, so one bad item doesn't abort the batch. Reuses the `EmbeddingModel`, `SnowflakeIdGenerator`, and `VectorStoreRepository` already wired into this service. |
| `repository/VectorStoreRepository.java` | New method `insertKnowledge(Long id, KnowledgeImportItem item, float[] embedding)`. Single raw-SQL `INSERT` (via `JdbcTemplate`, not JPA) covering all columns including `embedding::vector`, reusing the existing private `toVectorString(float[])` helper. |

## Data Flow

```
KnowledgeController.importBatch(List<KnowledgeImportItem>)
  └─► KnowledgeIngestionService.importBatch(items)
        for each item:
          ├─► EmbeddingModel.embed(item.chunkText())        → float[768]
          ├─► SnowflakeIdGenerator.nextId()
          └─► VectorStoreRepository.insertKnowledge(id, item, embedding)
                └─► JdbcTemplate raw INSERT ... VALUES (..., ?::vector, ...)
        catch per item → log.error + add to failedItems, continue loop
        return KnowledgeImportResult(successCount, failCount, failedItems)
```

## SQL (exact, per requirement)

```sql
INSERT INTO aiot_knowledge (id, title, doc_type, product_id, device_model,
  chunk_text, embedding, source, creator, tenant_id, deleted, create_time, update_time)
VALUES (?, ?, ?, ?, ?, ?, ?::vector, ?, 'system', 1, 0, now(), now())
```

`embedding` param is the `[0.1,0.2,...]` string produced by the existing `VectorStoreRepository.toVectorString(float[])` helper, unchanged.

## Error Handling

- Per-item try/catch in `importBatch` — DB constraint violations (e.g. missing required `doc_type`, since the column is `NOT NULL smallint`), embedding-call failures (Ollama unreachable), etc. are all caught, logged via `log.error`, and recorded as a `FailedItem` with the exception message. No extra validation code — the DB constraints double as validation.
- The endpoint never throws for a partially-bad batch; it always returns `200` with counts.

## Out of Scope

- No batching of the embedding calls (Spring AI's `embed(List<String>)`) — 15 calls to a local Ollama instance is fast enough (few seconds), and per-item embedding keeps the try/catch granularity at the item level, per the stated requirement ("15 条数据每条调一次 embedding API").
- No new validation framework (`@Valid`, Bean Validation) — DB constraints are sufficient for this internal/admin-facing bulk-load endpoint.
- No changes to `RagController`, `/api/rag/ingest`, or the existing single-document ingestion/chunking logic.

## Testing

- Unit test for `KnowledgeIngestionService.importBatch` with a mocked `EmbeddingModel` and `VectorStoreRepository`: verify success path (N items → N inserts, successCount == N) and partial-failure path (one item's embed/insert throws → failCount == 1, failedItems has 1 entry, remaining items still processed).
- Manual verification: `POST /api/knowledge/import` with a small JSON array against the running app (Ollama + Postgres up), confirm rows land in `aiot_knowledge` with correct `embedding` vector via `psql`.
