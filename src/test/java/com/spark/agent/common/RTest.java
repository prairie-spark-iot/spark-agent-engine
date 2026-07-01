package com.spark.agent.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RTest {

    @Test
    void ok_createsSuccessResponse() {
        R<String> r = R.ok("hello");
        assertEquals(0, r.getCode());
        assertEquals("success", r.getMsg());
        assertEquals("hello", r.getData());
    }

    @Test
    void ok_withNullData() {
        R<Object> r = R.ok(null);
        assertEquals(0, r.getCode());
        assertEquals("success", r.getMsg());
        assertNull(r.getData());
    }

    @Test
    void ok_withListData() {
        List<Integer> data = List.of(1, 2, 3);
        R<List<Integer>> r = R.ok(data);
        assertEquals(0, r.getCode());
        assertEquals(data, r.getData());
    }

    @Test
    void fail_creates500Response() {
        R<Void> r = R.fail("something went wrong");
        assertEquals(500, r.getCode());
        assertEquals("something went wrong", r.getMsg());
        assertNull(r.getData());
    }

    @Test
    void fail_withCustomCode() {
        R<Void> r = R.fail(404, "not found");
        assertEquals(404, r.getCode());
        assertEquals("not found", r.getMsg());
        assertNull(r.getData());
    }

    @Test
    void fail_with400Code() {
        R<Void> r = R.fail(400, "bad request");
        assertEquals(400, r.getCode());
        assertEquals("bad request", r.getMsg());
    }

    @Test
    void constructor_setsFields() {
        R<String> r = new R<>(1, "custom", "data");
        assertEquals(1, r.getCode());
        assertEquals("custom", r.getMsg());
        assertEquals("data", r.getData());
    }
}
