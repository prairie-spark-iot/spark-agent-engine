package com.spark.agent.service;

import com.spark.agent.config.AppProperties;
import com.spark.agent.repository.DeviceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceHeartbeatService implements MessageListener {

    private final StringRedisTemplate redisTemplate;
    private final DeviceRepository deviceRepository;
    private final AppProperties appProperties;

    /**
     * Called on every MQTT message. Writes to DB only on offline→online transition
     * (when Redis key was absent); otherwise just refreshes the key TTL.
     */
    public void heartbeat(Long deviceId, String deviceKey) {
        String key = appProperties.getDeviceHeartbeatKeyPrefix() + deviceKey;
        Duration ttl = Duration.ofSeconds(appProperties.getDeviceHeartbeatTtlSeconds());

        // SET NX EX: returns true only if the key did not exist (device was offline)
        Boolean wasAbsent = redisTemplate.opsForValue().setIfAbsent(key, "1", ttl);
        if (Boolean.TRUE.equals(wasAbsent)) {
            deviceRepository.markOnline(deviceId, LocalDateTime.now());
            log.info("[Heartbeat] {} came online", deviceKey);
        } else {
            // device already online — push the expiry window, no DB write
            redisTemplate.expire(key, ttl);
        }
    }

    /**
     * Immediately marks a device offline (bypassing the Redis TTL wait), triggered by an
     * explicit device/offline/{deviceKey} MQTT event.
     */
    @Transactional
    public void markOfflineNow(String deviceKey) {
        String key = appProperties.getDeviceHeartbeatKeyPrefix() + deviceKey;
        redisTemplate.delete(key);
        deviceRepository.findByDeviceKeyAndDeleted(deviceKey, (short) 0)
                .ifPresent(device -> {
                    deviceRepository.markOffline(device.getId(), LocalDateTime.now());
                    log.info("[Heartbeat] {} went offline (explicit event)", deviceKey);
                });
    }

    /**
     * Marks every currently-online device offline, triggered by the emulator's crash LWT
     * (device/offline/emulator) — one process death affects every device it was managing.
     */
    @Transactional
    public void markAllOffline() {
        LocalDateTime now = LocalDateTime.now();
        deviceRepository.findByOnlineStatusAndDeleted((short) 1, (short) 0)
                .forEach(device -> {
                    redisTemplate.delete(appProperties.getDeviceHeartbeatKeyPrefix() + device.getDeviceKey());
                    deviceRepository.markOffline(device.getId(), now);
                    log.info("[Heartbeat] {} went offline (emulator crash)", device.getDeviceKey());
                });
    }

    /**
     * Invoked by RedisMessageListenerContainer when a device:online:{key} expires.
     * Transitions the device to offline in DB (online→offline state change only).
     */
    @Override
    @Transactional
    public void onMessage(Message message, byte[] pattern) {
        String expiredKey = new String(message.getBody(), StandardCharsets.UTF_8);
        String prefix = appProperties.getDeviceHeartbeatKeyPrefix();
        if (!expiredKey.startsWith(prefix)) {
            return;
        }
        String deviceKey = expiredKey.substring(prefix.length());
        // Check if the device has already reconnected (new heartbeat key exists).
        // Redis expiry notification may arrive after the device already sent a new heartbeat,
        // so verify the key is still absent before marking offline.
        String redisKey = prefix + deviceKey;
        Boolean stillAbsent = redisTemplate.hasKey(redisKey);
        if (Boolean.TRUE.equals(stillAbsent)) {
            return;
        }
        deviceRepository.findByDeviceKeyAndDeleted(deviceKey, (short) 0)
                .ifPresent(device -> {
                    deviceRepository.markOffline(device.getId(), LocalDateTime.now());
                    log.info("[Heartbeat] {} went offline (key expired)", deviceKey);
                });
    }
}
