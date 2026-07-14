package com.spark.agent.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Guards spring.kafka.producer.transaction-id-prefix in application.yaml: without it, Boot
 * never registers a KafkaTransactionManager bean and this injection fails with
 * NoSuchBeanDefinitionException instead of a plain assertion failure.
 */
@SpringBootTest
class KafkaTransactionManagerTest {

    @Autowired
    private KafkaTransactionManager<Object, Object> kafkaTransactionManager;

    @Test
    void kafkaTransactionManagerBeanIsAutoConfigured() {
        assertNotNull(kafkaTransactionManager);
    }
}
