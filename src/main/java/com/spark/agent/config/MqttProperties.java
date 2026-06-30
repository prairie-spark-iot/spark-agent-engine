package com.spark.agent.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "mqtt")
public class MqttProperties {
    private String host = "localhost";
    private int port = 1883;
    private String topic = "/sys/+/+/thing/event/property/post";
    private String clientIdPrefix = "spark-agent";
}
