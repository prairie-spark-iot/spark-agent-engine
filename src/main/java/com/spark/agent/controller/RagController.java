package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.repository.VectorStoreRepository;
import com.spark.agent.service.KnowledgeIngestionService;
import com.spark.agent.service.RagSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/rag")
@RequiredArgsConstructor
public class RagController {

    private final KnowledgeIngestionService ingestionService;
    private final RagSearchService searchService;

    record IngestRequest(String title, String content, Short docType,
                         String deviceModel, Long productId, String source) {}

    record SearchRequest(String query, String deviceModel, int topK) {}

    @PostMapping("/ingest")
    public R<Map<String, Integer>> ingest(@RequestBody IngestRequest req) {
        int count = ingestionService.ingest(req.title(), req.content(), req.docType(),
                req.deviceModel(), req.productId(), req.source());
        return R.ok(Map.of("chunks", count));
    }

    @PostMapping("/search")
    public R<List<VectorStoreRepository.SearchResult>> search(@RequestBody SearchRequest req) {
        if (req.query() == null || req.query().isBlank()) {
            return R.fail("query must not be empty");
        }
        int topK = req.topK() > 0 ? Math.min(req.topK(), 20) : 5;
        return R.ok(searchService.search(req.query(), req.deviceModel(), topK));
    }
}
