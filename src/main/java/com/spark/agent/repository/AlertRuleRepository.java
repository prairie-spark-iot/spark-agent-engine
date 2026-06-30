package com.spark.agent.repository;

import com.spark.agent.entity.AlertRule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface AlertRuleRepository extends JpaRepository<AlertRule, Long> {

    @Query("""
            SELECT r FROM AlertRule r
            WHERE r.status = 1 AND r.deleted = 0
              AND r.identifier = :identifier
              AND (r.deviceId = :deviceId OR r.deviceId IS NULL)
            """)
    List<AlertRule> findActiveRules(Long deviceId, String identifier);
}
