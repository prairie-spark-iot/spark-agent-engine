package com.spark.agent.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AlertOperatorConverterTest {

    private final AlertOperatorConverter converter = new AlertOperatorConverter();

    @Test
    void convertToDatabaseColumn_returnsCode() {
        assertEquals("gt", converter.convertToDatabaseColumn(AlertOperator.GT));
        assertEquals("ne", converter.convertToDatabaseColumn(AlertOperator.NE));
    }

    @Test
    void convertToDatabaseColumn_nullAttributeReturnsNull() {
        assertNull(converter.convertToDatabaseColumn(null));
    }

    @Test
    void convertToEntityAttribute_mapsKnownCodeToConstant() {
        assertEquals(AlertOperator.GT, converter.convertToEntityAttribute("gt"));
        assertEquals(AlertOperator.LTE, converter.convertToEntityAttribute("lte"));
    }

    @Test
    void convertToEntityAttribute_unknownCodeMapsToUnknownConstant() {
        assertEquals(AlertOperator.UNKNOWN, converter.convertToEntityAttribute("invalid_op"));
    }

    @Test
    void convertToEntityAttribute_nullDbDataReturnsNull() {
        assertNull(converter.convertToEntityAttribute(null));
    }
}
