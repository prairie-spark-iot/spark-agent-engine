package com.spark.agent.config;

import com.spark.agent.service.DeviceHeartbeatService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Slf4j
@Configuration
public class RedisKeyExpirationConfig {

    @Bean
    public RedisMessageListenerContainer redisKeyExpirationListenerContainer(
            RedisConnectionFactory connectionFactory,
            DeviceHeartbeatService heartbeatService) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        // DB 0 is the default; if spring.data.redis.database is changed, update the topic index
        container.addMessageListener(heartbeatService, new ChannelTopic("__keyevent@0__:expired"));
        return container;
    }

    /**
     * Enable keyspace expiry events at startup via CONFIG SET.
     * "E" = keyevent notifications, "x" = expired key events.
     *
     * If your Redis instance has CONFIG SET disabled (ACL / cloud restriction), set it manually:
     *   redis-cli CONFIG SET notify-keyspace-events Ex
     */
    @Bean
    public InitializingBean enableRedisKeyspaceNotifications(StringRedisTemplate redisTemplate) {
        return () -> {
            try {
                redisTemplate.execute((RedisCallback<Void>) conn -> {
                    conn.serverCommands().setConfig("notify-keyspace-events", "Ex");
                    return null;
                });
                log.info("[Redis] Keyspace expiry notifications enabled (notify-keyspace-events=Ex)");
            } catch (Exception e) {
                log.warn("[Redis] Could not auto-configure keyspace notifications: {}. " +
                        "Set manually: redis-cli CONFIG SET notify-keyspace-events Ex", e.getMessage());
            }
        };
    }
}
