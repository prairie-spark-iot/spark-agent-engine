package com.spark.agent.mqtt;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.spark.agent.config.MqttProperties;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@Component
public class MqttSubscriber implements ApplicationRunner {

    private final MqttProperties props;
    private Mqtt5AsyncClient client;

    public MqttSubscriber(MqttProperties props) {
        this.props = props;
    }

    @PostConstruct
    public void initClient() {
        client = Mqtt5Client.builder()
                .identifier(props.getClientIdPrefix() + "-" + UUID.randomUUID().toString().substring(0, 8))
                .serverHost(props.getHost())
                .serverPort(props.getPort())
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(ctx -> subscribe())
                .addDisconnectedListener(ctx -> {
                    Throwable cause = ctx.getCause();
                    log.warn("[MQTT] Disconnected: {}", cause != null ? cause.getMessage() : "unknown");
                })
                .buildAsync();
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("[MQTT] Connecting to {}:{}", props.getHost(), props.getPort());
        client.connectWith()
                .cleanStart(true)
                .send()
                .exceptionally(ex -> {
                    log.error("[MQTT] Initial connection failed", ex);
                    return null;
                });
    }

    private void subscribe() {
        log.info("[MQTT] Subscribing to {}", props.getTopic());
        client.subscribeWith()
                .topicFilter(props.getTopic())
                .qos(MqttQos.AT_LEAST_ONCE)
                .callback(this::handleMessage)
                .send()
                .thenAccept(ack -> log.info("[MQTT] Subscribed: {}", ack.getReasonCodes()));
    }

    // ponytail: placeholder — will be replaced with TelemetryService dispatch in Step 3
    private void handleMessage(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            log.info("[MQTT] topic={} payload={}", message.getTopic(), payload);
        } catch (Exception e) {
            log.error("[MQTT] Error processing message", e);
        }
    }
}
