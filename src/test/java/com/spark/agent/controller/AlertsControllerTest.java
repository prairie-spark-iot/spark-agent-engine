package com.spark.agent.controller;

import com.spark.agent.common.ConflictException;
import com.spark.agent.common.R;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.service.AlertService;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AlertsControllerTest {

    @Mock
    private AlertService alertService;

    @InjectMocks
    private AlertsController controller;

    @Test
    void requestDiagnosis_returns202WithDiagnosingStatus() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        when(alertService.requestDiagnosis(42L)).thenReturn(record);

        ResponseEntity<R<AlertsController.DiagnoseResponseData>> resp = controller.requestDiagnosis(42L);

        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertEquals(0, resp.getBody().getCode());
        assertEquals("42", resp.getBody().getData().id());
        assertEquals("Diagnosing", resp.getBody().getData().status());
    }

    @Test
    void requestDiagnosis_alertNotFound_propagatesException() {
        when(alertService.requestDiagnosis(999L)).thenThrow(new EntityNotFoundException("AlertRecord 999 not found"));

        assertThrows(EntityNotFoundException.class, () -> controller.requestDiagnosis(999L));
    }

    @Test
    void requestDiagnosis_alreadyDiagnosing_propagatesConflict() {
        when(alertService.requestDiagnosis(42L)).thenThrow(new ConflictException("Alert 42 is already Diagnosing or Diagnosed"));

        assertThrows(ConflictException.class, () -> controller.requestDiagnosis(42L));
    }

    @Test
    void approve_returns200WithUpdatedAlert() {
        AlertRecord record = new AlertRecord();
        record.setId(42L);
        record.setDiagnosisStatus((short) 2);
        record.setHandleStatus((short) 1);
        record.setApprovedAt(java.time.LocalDateTime.of(2026, 7, 5, 20, 0));
        when(alertService.approveAlert(42L)).thenReturn(record);

        ResponseEntity<R<com.spark.agent.dto.AlertRecordResponse>> resp = controller.approve(42L);

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertEquals(0, resp.getBody().getCode());
        assertEquals(42L, resp.getBody().getData().id());
        assertEquals((short) 1, resp.getBody().getData().handleStatus());
        assertNotNull(resp.getBody().getData().approvedAt());
    }

    @Test
    void approve_alertNotFound_propagatesException() {
        when(alertService.approveAlert(999L)).thenThrow(new EntityNotFoundException("AlertRecord 999 not found"));

        assertThrows(EntityNotFoundException.class, () -> controller.approve(999L));
    }

    @Test
    void approve_stillPending_propagatesConflict() {
        when(alertService.approveAlert(42L)).thenThrow(new ConflictException("Alert 42 has not been diagnosed yet"));

        assertThrows(ConflictException.class, () -> controller.approve(42L));
    }
}
