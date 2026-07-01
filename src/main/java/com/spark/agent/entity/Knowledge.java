package com.spark.agent.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "aiot_knowledge")
public class Knowledge extends BaseEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "title")
    private String title;

    @Column(name = "chunk_text", columnDefinition = "text")
    private String chunkText;

    @Column(name = "doc_type")
    private Short docType;

    @Column(name = "device_model")
    private String deviceModel;

    @Column(name = "product_id")
    private Long productId;

    @Column(name = "source")
    private String source;
}
