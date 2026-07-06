package com.spark.agent.common;

import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void handleNotFound_returns404() {
        EntityNotFoundException ex = new EntityNotFoundException("Device not found");

        ResponseEntity<R<Void>> resp = handler.handleNotFound(ex);

        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
        assertEquals(404, resp.getBody().getCode());
        assertEquals("Device not found", resp.getBody().getMsg());
    }

    @Test
    void handleBadRequest_returns400() {
        IllegalArgumentException ex = new IllegalArgumentException("invalid param");

        ResponseEntity<R<Void>> resp = handler.handleBadRequest(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals(400, resp.getBody().getCode());
        assertEquals("invalid param", resp.getBody().getMsg());
    }

    @Test
    void handleTypeMismatch_returns400WithParamName() {
        MethodArgumentTypeMismatchException ex = mock(MethodArgumentTypeMismatchException.class);
        when(ex.getName()).thenReturn("deviceKey");
        when(ex.getMessage()).thenReturn("Failed to convert value");

        ResponseEntity<R<Void>> resp = handler.handleTypeMismatch(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals(400, resp.getBody().getCode());
        assertEquals("Invalid parameter: deviceKey", resp.getBody().getMsg());
    }

    @Test
    void handleNotReadable_returns400() {
        // Use mock because constructor signature differs between Spring versions
        var ex = mock(org.springframework.http.converter.HttpMessageNotReadableException.class);
        when(ex.getMessage()).thenReturn("Malformed JSON");

        ResponseEntity<R<Void>> resp = handler.handleNotReadable(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals(400, resp.getBody().getCode());
        assertEquals("Malformed request body", resp.getBody().getMsg());
    }

    @Test
    void handleValidation_returns400WithFieldErrors() {
        MethodArgumentNotValidException ex = mock(MethodArgumentNotValidException.class);
        var bindingResult = mock(org.springframework.validation.BindingResult.class);
        when(ex.getBindingResult()).thenReturn(bindingResult);
        when(bindingResult.getFieldErrors()).thenReturn(List.of(
                new org.springframework.validation.FieldError("obj", "title", "must not be blank"),
                new org.springframework.validation.FieldError("obj", "content", "must not be blank")
        ));

        ResponseEntity<R<Void>> resp = handler.handleValidation(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals(400, resp.getBody().getCode());
        assertTrue(resp.getBody().getMsg().contains("title: must not be blank"));
        assertTrue(resp.getBody().getMsg().contains("content: must not be blank"));
    }

    @Test
    void handleValidation_noFieldErrors_fallsBack() {
        MethodArgumentNotValidException ex = mock(MethodArgumentNotValidException.class);
        var bindingResult = mock(org.springframework.validation.BindingResult.class);
        when(ex.getBindingResult()).thenReturn(bindingResult);
        when(bindingResult.getFieldErrors()).thenReturn(List.of());

        ResponseEntity<R<Void>> resp = handler.handleValidation(ex);

        assertEquals(400, resp.getBody().getCode());
        assertEquals("Validation failed", resp.getBody().getMsg());
    }

    @Test
    void handleConstraintViolation_returns400() {
        ConstraintViolation<?> violation = mock(ConstraintViolation.class);
        when(violation.getMessage()).thenReturn("title: must not be blank");
        ConstraintViolationException ex = new ConstraintViolationException("validation failed", Set.of(violation));

        ResponseEntity<R<Void>> resp = handler.handleConstraintViolation(ex);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals(400, resp.getBody().getCode());
    }

    @Test
    void handleConflict_returns409() {
        ConflictException ex = new ConflictException("Alert 42 is already Diagnosing or Diagnosed");

        ResponseEntity<R<Void>> resp = handler.handleConflict(ex);

        assertEquals(HttpStatus.CONFLICT, resp.getStatusCode());
        assertEquals(409, resp.getBody().getCode());
        assertEquals("Alert 42 is already Diagnosing or Diagnosed", resp.getBody().getMsg());
    }

    @Test
    void handleGeneral_returns500() {
        Exception ex = new RuntimeException("Unexpected error");

        ResponseEntity<R<Void>> resp = handler.handleGeneral(ex);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getStatusCode());
        assertEquals(500, resp.getBody().getCode());
        assertEquals("Internal server error", resp.getBody().getMsg());
    }
}
