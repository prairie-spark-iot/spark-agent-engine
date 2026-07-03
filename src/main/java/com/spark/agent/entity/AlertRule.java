package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "aiot_alert_rule")
public class AlertRule extends BaseEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "product_id")
    private Long productId;

    @Column(name = "device_id")
    private Long deviceId;

    @Column(name = "identifier", nullable = false)
    private String identifier;

    /** gt / lt / gte / lte / eq / ne */
    @Column(name = "operator", nullable = false)
    @Convert(converter = AlertOperatorConverter.class)
    private AlertOperator operator;

    /** stored as varchar in DB */
    @Column(name = "threshold", nullable = false)
    private String threshold;

    /** 1=info 2=warning 3=critical */
    @Column(name = "level", nullable = false)
    private Short level = 1;

    @Column(name = "description")
    private String description;

    /** 1=enabled */
    @Column(name = "status", nullable = false)
    private Short status = 1;
}
