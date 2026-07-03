package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.OutboxMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxMessageFactoryTest {

    @Mock
    private SnowflakeIdGenerator idGenerator;

    private ObjectMapper objectMapper;
    private OutboxMessageFactory factory;

    record SamplePayload(String deviceKey, String identifier) {}

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        factory = new OutboxMessageFactory(idGenerator, objectMapper);
    }

    @Test
    void build_populatesAllFieldsAndSerializesPayload() {
        when(idGenerator.nextId()).thenReturn(555L);
        SamplePayload payload = new SamplePayload("DK_TEST_001", "temperature");

        OutboxMessage msg = factory.build("device_data", "123", "device.data", payload);

        assertEquals(555L, msg.getId());
        assertEquals("device_data", msg.getAggregateType());
        assertEquals("123", msg.getAggregateId());
        assertEquals("device.data", msg.getEventType());
        assertNull(msg.getPublishedAt());
        assertNotNull(msg.getCreatedAt());
        assertTrue(msg.getPayload().contains("DK_TEST_001"));
        assertTrue(msg.getPayload().contains("temperature"));
    }
}
