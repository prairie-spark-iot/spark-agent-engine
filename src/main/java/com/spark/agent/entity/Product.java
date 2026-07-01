package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "aiot_product")
public class Product extends BaseEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "product_key")
    private String productKey;
}
