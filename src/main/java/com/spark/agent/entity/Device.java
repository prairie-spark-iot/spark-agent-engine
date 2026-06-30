package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "aiot_device")
public class Device extends BaseEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "device_name", nullable = false)
    private String deviceName;

    @Column(name = "device_key", nullable = false, unique = true)
    private String deviceKey;

    @Column(name = "online_status", nullable = false)
    private Short onlineStatus = 0;

    @Column(name = "last_online_time")
    private LocalDateTime lastOnlineTime;

    @Column(name = "last_offline_time")
    private LocalDateTime lastOfflineTime;

    @Column(name = "status", nullable = false)
    private Short status = 0;
}
