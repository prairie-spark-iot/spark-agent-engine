package com.spark.agent.service;

import com.spark.agent.repository.VectorStoreRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class RagSearchService {

    private final EmbeddingModel embeddingModel;
    private final VectorStoreRepository vectorStoreRepository;

    public List<VectorStoreRepository.SearchResult> search(String query, String deviceModel, int topK) {
        float[] embedding = embeddingModel.embed(query);
        return vectorStoreRepository.search(embedding, deviceModel, topK);
    }
}
