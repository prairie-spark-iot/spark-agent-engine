package com.spark.agent.repository;

import com.spark.agent.entity.DeviceData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface DeviceDataRepository extends JpaRepository<DeviceData, Long> {

    @Query("""
            SELECT d FROM DeviceData d
            WHERE d.deviceKey = :deviceKey AND d.deleted = 0
              AND d.reportTime = (
                SELECT MAX(d2.reportTime) FROM DeviceData d2
                WHERE d2.deviceKey = :deviceKey AND d2.identifier = d.identifier AND d2.deleted = 0
              )
            ORDER BY d.identifier
            """)
    List<DeviceData> findLatestByDeviceKey(String deviceKey);

    List<DeviceData> findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
            String deviceKey, String identifier, Short deleted, org.springframework.data.domain.Pageable pageable);
}
