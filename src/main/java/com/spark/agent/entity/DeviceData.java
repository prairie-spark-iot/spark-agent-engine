package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "aiot_device_data")
public class DeviceData extends BaseEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "device_id", nullable = false)
    private Long deviceId;

    @Column(name = "device_key", nullable = false)
    private String deviceKey;

    @Column(name = "identifier", nullable = false)
    private String identifier;

    @Column(name = "value")
    private String value;

    @Column(name = "value_num", precision = 20, scale = 4)
    private BigDecimal valueNum;

    @Column(name = "quality", nullable = false)
    private Short quality = 1;

    @Column(name = "report_time", nullable = false)
    private LocalDateTime reportTime;
}
