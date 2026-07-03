package com.spark.agent.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public CompletableFuture<SendResult<Object, Object>> sendRaw(String topic, String key, String jsonPayload) {
        return kafkaTemplate.send(topic, key, jsonPayload);
    }
}
