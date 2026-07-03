package com.spark.agent.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AlertOperatorTest {

    @Test
    void gt_matchesWhenAboveThreshold() {
        assertTrue(AlertOperator.GT.matches(101, 100));
        assertFalse(AlertOperator.GT.matches(100, 100));
        assertFalse(AlertOperator.GT.matches(99, 100));
    }

    @Test
    void lt_matchesWhenBelowThreshold() {
        assertTrue(AlertOperator.LT.matches(99, 100));
        assertFalse(AlertOperator.LT.matches(100, 100));
        assertFalse(AlertOperator.LT.matches(101, 100));
    }

    @Test
    void gte_matchesWhenAboveOrEqualThreshold() {
        assertTrue(AlertOperator.GTE.matches(101, 100));
        assertTrue(AlertOperator.GTE.matches(100, 100));
        assertFalse(AlertOperator.GTE.matches(99, 100));
    }

    @Test
    void lte_matchesWhenBelowOrEqualThreshold() {
        assertTrue(AlertOperator.LTE.matches(99, 100));
        assertTrue(AlertOperator.LTE.matches(100, 100));
        assertFalse(AlertOperator.LTE.matches(101, 100));
    }

    @Test
    void eq_matchesWithinEpsilon() {
        assertTrue(AlertOperator.EQ.matches(99.9, 99.9));
        assertFalse(AlertOperator.EQ.matches(99.9001, 99.9));
    }

    @Test
    void ne_matchesOutsideEpsilon() {
        assertTrue(AlertOperator.NE.matches(50, 100));
        assertFalse(AlertOperator.NE.matches(99.9, 99.9));
    }

    @Test
    void unknown_neverMatches() {
        assertFalse(AlertOperator.UNKNOWN.matches(100, 100));
        assertFalse(AlertOperator.UNKNOWN.matches(0, 0));
    }

    @Test
    void code_returnsDbLowercaseCode() {
        assertEquals("gt", AlertOperator.GT.code());
        assertEquals("lt", AlertOperator.LT.code());
        assertEquals("gte", AlertOperator.GTE.code());
        assertEquals("lte", AlertOperator.LTE.code());
        assertEquals("eq", AlertOperator.EQ.code());
        assertEquals("ne", AlertOperator.NE.code());
    }

    @Test
    void fromCode_returnsMatchingConstantForValidCodes() {
        assertEquals(AlertOperator.GT, AlertOperator.fromCode("gt"));
        assertEquals(AlertOperator.LT, AlertOperator.fromCode("lt"));
        assertEquals(AlertOperator.GTE, AlertOperator.fromCode("gte"));
        assertEquals(AlertOperator.LTE, AlertOperator.fromCode("lte"));
        assertEquals(AlertOperator.EQ, AlertOperator.fromCode("eq"));
        assertEquals(AlertOperator.NE, AlertOperator.fromCode("ne"));
    }

    @Test
    void fromCode_returnsNullForUnrecognizedCode() {
        assertNull(AlertOperator.fromCode("invalid_op"));
    }
}
