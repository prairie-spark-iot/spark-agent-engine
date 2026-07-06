package com.spark.agent.common;

/** Generic HTTP 409 signal — mapped by GlobalExceptionHandler, mirroring how IllegalArgumentException maps to 400. */
public class ConflictException extends RuntimeException {
    public ConflictException(String message) {
        super(message);
    }
}
