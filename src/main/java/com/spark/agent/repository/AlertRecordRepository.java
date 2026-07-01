package com.spark.agent.repository;

import com.spark.agent.entity.AlertRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;

public interface AlertRecordRepository extends JpaRepository<AlertRecord, Long> {

    @Query("""
            SELECT COUNT(r) FROM AlertRecord r
            WHERE r.deviceId = :deviceId AND r.ruleId = :ruleId
              AND r.handleStatus = 0 AND r.deleted = 0
              AND r.triggerTime >= :since
            """)
    long countRecentUnhandled(Long deviceId, Long ruleId, LocalDateTime since);

    List<AlertRecord> findByDeletedOrderByTriggerTimeDesc(Short deleted, org.springframework.data.domain.Pageable pageable);

    List<AlertRecord> findTop5ByDeviceKeyAndDeletedOrderByTriggerTimeDesc(String deviceKey, Short deleted);
}
