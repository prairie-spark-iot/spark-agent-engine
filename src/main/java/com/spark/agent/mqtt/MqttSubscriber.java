package com.spark.agent.mqtt;

import tools.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.spark.agent.config.MqttProperties;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.service.DeviceHeartbeatService;
import com.spark.agent.service.TelemetryService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class MqttSubscriber implements ApplicationRunner {

    private final MqttProperties props;
    private final TelemetryService telemetryService;
    private final DeviceRepository deviceRepository;
    private final DeviceHeartbeatService heartbeatService;
    private final ObjectMapper objectMapper;
    private Mqtt5AsyncClient client;
    private final Executor mqttExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public MqttSubscriber(MqttProperties props, TelemetryService telemetryService,
                           DeviceRepository deviceRepository, DeviceHeartbeatService heartbeatService,
                           ObjectMapper objectMapper) {
        this.props = props;
        this.telemetryService = telemetryService;
        this.deviceRepository = deviceRepository;
        this.heartbeatService = heartbeatService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initClient() {
        client = Mqtt5Client.builder()
                .identifier(props.getClientIdPrefix() + "-" + UUID.randomUUID().toString().replace("-", ""))
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
        try {
            client.connectWith()
                    .cleanStart(true)
                    .send()
                    .get();
            log.info("[MQTT] Connected successfully");
        } catch (Exception e) {
            log.error("[MQTT] Initial connection failed: {}", e.getMessage());
            throw new RuntimeException("MQTT initial connection failed, aborting startup", e);
        }
    }

    private void subscribe() {
        subscribeTo(props.getTopic(), this::handleMessage);
        subscribeTo(props.getOnlineTopic(), this::handleOnlineMessage);
        subscribeTo(props.getOfflineTopic(), this::handleOfflineMessage);
        subscribeTo(props.getStatusTopic(), this::handleStatusMessage);
    }

    private void subscribeTo(String topicFilter, java.util.function.Consumer<Mqtt5Publish> callback) {
        log.info("[MQTT] Subscribing to {}", topicFilter);
        client.subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .callback(callback::accept)
                .send()
                .thenAccept(ack -> log.info("[MQTT] Subscribed to {}: {}", topicFilter, ack.getReasonCodes()));
    }

    @PreDestroy
    public void destroy() {
        if (client != null) {
            client.disconnect();
            log.info("[MQTT] Client disconnected");
        }
    }

    private void handleMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatch(message));
    }

    private void handleOnlineMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatchOnline(message));
    }

    private void handleOfflineMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatchOffline(message));
    }

    private void handleStatusMessage(Mqtt5Publish message) {
        mqttExecutor.execute(() -> dispatchStatus(message));
    }

    void dispatch(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceTelemetryMessage msg = objectMapper.readValue(payload, DeviceTelemetryMessage.class);
            telemetryService.process(msg);
        } catch (Exception e) {
            log.error("[MQTT] Error processing message from {}: {}", message.getTopic(), e.getMessage());
        }
    }

    void dispatchOnline(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceConnectionEventMessage msg = objectMapper.readValue(payload, DeviceConnectionEventMessage.class);
            deviceRepository.findByDeviceKeyAndDeleted(msg.getDeviceKey(), (short) 0)
                    .ifPresentOrElse(
                            device -> heartbeatService.heartbeat(device.getId(), device.getDeviceKey()),
                            () -> log.warn("[MQTT] Online event for unknown device: {}", msg.getDeviceKey()));
        } catch (Exception e) {
            log.error("[MQTT] Error processing online event from {}: {}", message.getTopic(), e.getMessage());
        }
    }

    void dispatchOffline(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceConnectionEventMessage msg = objectMapper.readValue(payload, DeviceConnectionEventMessage.class);
            if ("emulator".equals(msg.getDeviceKey())) {
                heartbeatService.markAllOffline();
            } else {
                heartbeatService.markOfflineNow(msg.getDeviceKey());
            }
        } catch (Exception e) {
            log.error("[MQTT] Error processing offline event from {}: {}", message.getTopic(), e.getMessage());
        }
    }

    void dispatchStatus(Mqtt5Publish message) {
        try {
            String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
            DeviceStatusEventMessage msg = objectMapper.readValue(payload, DeviceStatusEventMessage.class);
            deviceRepository.findByDeviceKeyAndDeleted(msg.getDeviceKey(), (short) 0)
                    .ifPresentOrElse(
                            device -> heartbeatService.heartbeat(device.getId(), device.getDeviceKey()),
                            () -> log.warn("[MQTT] Status event for unknown device: {}", msg.getDeviceKey()));
        } catch (Exception e) {
            log.error("[MQTT] Error processing status event from {}: {}", message.getTopic(), e.getMessage());
        }
    }
}
