package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.dto.AlertRecordResponse;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.service.AlertService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * New plural /api/alerts resource group for Phase 1/2 integration-contract endpoints — kept
 * separate from the legacy singular /api/alert/recent in ApiController rather than merged into
 * it. See spark-agent-docs/phase-1-2-api-data-contracts.md.
 */
@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertsController {

    private final AlertService alertService;

    public record DiagnoseResponseData(String id, String status) {
    }

    /**
     * 202 Accepted, not synchronous or SSE: diagnosis runs through the existing
     * iot.alert.triggered outbox/Kafka pipeline (concurrency=1, matched to Ollama's single
     * inference slot), so the caller polls the existing GET /api/alert/recent /
     * telemetry-sync path for completion rather than waiting on this request.
     */
    @PostMapping("/{id}/diagnose")
    public ResponseEntity<R<DiagnoseResponseData>> requestDiagnosis(@PathVariable Long id) {
        AlertRecord record = alertService.requestDiagnosis(id);
        DiagnoseResponseData data = new DiagnoseResponseData(String.valueOf(record.getId()), "Diagnosing");
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(R.ok(data));
    }

    /**
     * 200 OK, synchronous — unlike /diagnose this is a plain DB update with no LLM call, so
     * there's no concurrency guard to route around and no reason to make the caller poll.
     */
    @PostMapping("/{id}/approve")
    public ResponseEntity<R<AlertRecordResponse>> approve(@PathVariable Long id) {
        AlertRecord record = alertService.approveAlert(id);
        return ResponseEntity.ok(R.ok(AlertRecordResponse.from(record)));
    }
}
