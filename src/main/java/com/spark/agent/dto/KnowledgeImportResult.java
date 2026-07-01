package com.spark.agent.dto;

import java.util.List;

public record KnowledgeImportResult(int successCount, int failCount, List<KnowledgeImportResult.FailedItem> failedItems) {
    public record FailedItem(String title, String reason) {
    }
}
