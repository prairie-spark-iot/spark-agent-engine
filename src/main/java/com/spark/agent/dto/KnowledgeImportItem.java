package com.spark.agent.dto;

public record KnowledgeImportItem(String title, Long productId, String deviceModel,
                                   Short docType, String source, String chunkText) {
}
