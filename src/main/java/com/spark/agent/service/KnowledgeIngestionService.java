package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.dto.KnowledgeImportItem;
import com.spark.agent.dto.KnowledgeImportResult;
import com.spark.agent.dto.KnowledgeImportResult.FailedItem;
import com.spark.agent.entity.Knowledge;
import com.spark.agent.repository.KnowledgeRepository;
import com.spark.agent.repository.VectorStoreRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class KnowledgeIngestionService {

    private final EmbeddingModel embeddingModel;
    private final KnowledgeRepository knowledgeRepository;
    private final VectorStoreRepository vectorStoreRepository;
    private final SnowflakeIdGenerator idGen;

    private static final int CHUNK_SIZE = 500;

    // Embed all chunks before opening any DB connection to avoid holding HikariCP connections during Ollama HTTP calls.
    @Transactional
    public int ingest(String title, String content, Short docType,
                      String deviceModel, Long productId, String source) {
        List<String> chunks = chunk(content);
        List<float[]> embeddings = chunks.stream().map(embeddingModel::embed).toList();

        for (int i = 0; i < chunks.size(); i++) {
            Knowledge k = new Knowledge();
            k.setId(idGen.nextId());
            k.setTitle(title);
            k.setChunkText(chunks.get(i));
            k.setDocType(docType);
            k.setDeviceModel(deviceModel);
            k.setProductId(productId);
            k.setSource(source);
            knowledgeRepository.save(k);
            vectorStoreRepository.saveEmbedding(k.getId(), embeddings.get(i));
        }

        log.info("[RAG] Ingested '{}' → {} chunk(s)", title, chunks.size());
        return chunks.size();
    }

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

    private List<String> chunk(String text) {
        List<String> result = new ArrayList<>();
        String[] paragraphs = text.split("\\n\\n+");
        StringBuilder current = new StringBuilder();

        for (String para : paragraphs) {
            para = para.strip();
            if (para.isEmpty()) continue;

            if (para.length() > CHUNK_SIZE) {
                if (!current.isEmpty()) {
                    result.add(current.toString().strip());
                    current = new StringBuilder();
                }
                for (int i = 0; i < para.length(); i += CHUNK_SIZE) {
                    result.add(para.substring(i, Math.min(i + CHUNK_SIZE, para.length())));
                }
                continue;
            }

            if (current.length() + para.length() > CHUNK_SIZE && !current.isEmpty()) {
                result.add(current.toString().strip());
                current = new StringBuilder();
            }
            if (!current.isEmpty()) current.append("\n\n");
            current.append(para);
        }

        if (!current.isEmpty()) result.add(current.toString().strip());
        return result.isEmpty() ? List.of(text.strip()) : result;
    }
}
