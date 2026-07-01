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

    private static final int MAX_BATCH_SIZE = 50;

    @PostMapping("/import")
    public R<KnowledgeImportResult> importBatch(@RequestBody List<KnowledgeImportItem> items) {
        if (items == null || items.isEmpty()) {
            return R.fail(400, "Import list must not be empty");
        }
        if (items.size() > MAX_BATCH_SIZE) {
            return R.fail(400, "Batch size %d exceeds maximum %d".formatted(items.size(), MAX_BATCH_SIZE));
        }
        return R.ok(ingestionService.importBatch(items));
    }
}
