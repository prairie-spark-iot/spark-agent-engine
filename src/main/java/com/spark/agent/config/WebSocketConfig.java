package com.spark.agent.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * STOMP/WebSocket push layer for real-time telemetry, alerts, diagnosis, and device-status —
 * additive to the existing MQTT/Kafka pipeline, no business logic here.
 * See docs/superpowers/specs/2026-07-06-websocket-realtime-design.md.
 *
 * Origins are an exact allowlist (app.ws-allowed-origins), not a wildcard: unlike REST fetch/XHR,
 * a WebSocket handshake isn't subject to the browser's normal same-origin CORS enforcement, so a
 * wildcard here would let any origin open a connection and receive live telemetry/alert/diagnosis
 * pushes. There is still no CONNECT/SUBSCRIBE-level authentication in this pass — matching the
 * fact that no REST endpoint in this app has any either — see the spec's Authentication section
 * for the documented future seam (a ChannelInterceptor on the inbound STOMP channel).
 */
@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final AppProperties appProperties;

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOrigins(appProperties.getWsAllowedOrigins().toArray(new String[0]))
                .withSockJS();
    }
}
