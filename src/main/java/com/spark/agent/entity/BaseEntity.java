package com.spark.agent.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@MappedSuperclass
public abstract class BaseEntity {

    @JsonIgnore
    @Column(name = "creator")
    private String creator = "";

    @Column(name = "create_time", nullable = false)
    private LocalDateTime createTime;

    @JsonIgnore
    @Column(name = "updater")
    private String updater = "";

    @Column(name = "update_time", nullable = false)
    private LocalDateTime updateTime;

    @JsonIgnore
    @Column(name = "deleted", nullable = false)
    private Short deleted = 0;

    @JsonIgnore
    @Column(name = "tenant_id", nullable = false)
    private Long tenantId = 1L;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createTime = now;
        updateTime = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updateTime = LocalDateTime.now();
    }
}
