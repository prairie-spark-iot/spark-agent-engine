package com.spark.agent.repository;

import com.spark.agent.dto.KnowledgeImportItem;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class VectorStoreRepository {

    private final JdbcTemplate jdbc;

    public record SearchResult(Long id, String title, String chunkText, Short docType,
                               String deviceModel, String source, double distance) {}

    @Transactional
    public void saveEmbedding(Long id, float[] embedding) {
        jdbc.update("""
                INSERT INTO aiot_knowledge (id, embedding, creator, tenant_id, deleted, create_time, update_time)
                VALUES (?, ?::vector, 'system', 1, 0, now(), now())
                ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, update_time = now()
                """,
                id, toVectorString(embedding));
    }

    @Transactional
    public void insertKnowledge(Long id, KnowledgeImportItem item, float[] embedding) {
        jdbc.update("""
                INSERT INTO aiot_knowledge (id, title, doc_type, product_id, device_model,
                  chunk_text, embedding, source, creator, tenant_id, deleted, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?::vector, ?, 'system', 1, 0, now(), now())
                """,
                id, item.title(), item.docType(), item.productId(), item.deviceModel(),
                item.chunkText(), toVectorString(embedding), item.source());
    }

    public List<SearchResult> search(float[] queryEmbedding, String deviceModel, int topK) {
        List<Object> params = new ArrayList<>();
        params.add(toVectorString(queryEmbedding));

        List<String> conditions = new ArrayList<>();
        conditions.add("deleted = 0");
        if (deviceModel != null && !deviceModel.isBlank()) {
            conditions.add("device_model = ?");
            params.add(deviceModel);
        }
        params.add(topK);

        String sql = """
                SELECT id, title, chunk_text, doc_type, device_model, source,
                       (embedding <-> ?::vector) AS distance
                FROM aiot_knowledge
                WHERE %s
                ORDER BY distance
                LIMIT ?
                """.formatted(String.join(" AND ", conditions));

        return jdbc.query(sql, (rs, i) -> new SearchResult(
                rs.getLong("id"),
                rs.getString("title"),
                rs.getString("chunk_text"),
                rs.getShort("doc_type"),
                rs.getString("device_model"),
                rs.getString("source"),
                rs.getDouble("distance")
        ), params.toArray());
    }

    private static String toVectorString(float[] v) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.append(']').toString();
    }
}
