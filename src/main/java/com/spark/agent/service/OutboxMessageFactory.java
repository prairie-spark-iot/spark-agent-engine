package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.entity.OutboxMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class OutboxMessageFactory {

    private final SnowflakeIdGenerator idGenerator;
    private final ObjectMapper objectMapper;

    public OutboxMessage build(String aggregateType, String aggregateId, String eventType, Object entity) {
        OutboxMessage msg = new OutboxMessage();
        msg.setId(idGenerator.nextId());
        msg.setAggregateType(aggregateType);
        msg.setAggregateId(aggregateId);
        msg.setEventType(eventType);
        msg.setPayload(objectMapper.writeValueAsString(entity));
        msg.setCreatedAt(LocalDateTime.now());
        return msg;
    }
}
