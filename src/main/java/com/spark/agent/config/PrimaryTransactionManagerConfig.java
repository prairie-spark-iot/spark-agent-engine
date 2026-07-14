package com.spark.agent.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuring the Kafka transaction manager (see application.yaml's
 * spring.kafka.producer.transaction-id-prefix) added a second PlatformTransactionManager
 * bean (kafkaTransactionManager) next to JPA's. Every existing unqualified @Transactional
 * (AlertService, TelemetryService, OutboxPublisher, ...) must keep resolving to the JPA one
 * without each call site adding a manager qualifier, so mark it primary instead.
 */
@Configuration
public class PrimaryTransactionManagerConfig {

    @Bean
    static BeanFactoryPostProcessor primaryJpaTransactionManager() {
        return beanFactory -> beanFactory.getBeanDefinition("transactionManager").setPrimary(true);
    }
}
