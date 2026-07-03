package com.spark.agent.repository;

import com.spark.agent.entity.DeviceData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;

public interface DeviceDataRepository extends JpaRepository<DeviceData, Long> {

    @Query(value = """
            SELECT ranked.id, ranked.device_id, ranked.device_key, ranked.identifier, ranked.value,
                   ranked.value_num, ranked.quality, ranked.report_time, ranked.creator, ranked.create_time,
                   ranked.updater, ranked.update_time, ranked.deleted, ranked.tenant_id
            FROM (
                SELECT d.*,
                       ROW_NUMBER() OVER (PARTITION BY d.identifier ORDER BY d.report_time DESC) AS rn
                FROM aiot_device_data d
                WHERE d.device_key = :deviceKey AND d.deleted = 0
            ) ranked
            WHERE ranked.rn = 1
            ORDER BY ranked.identifier
            """, nativeQuery = true)
    List<DeviceData> findLatestByDeviceKey(String deviceKey);

    List<DeviceData> findByDeviceKeyAndIdentifierAndDeletedOrderByReportTimeDesc(
            String deviceKey, String identifier, Short deleted, org.springframework.data.domain.Pageable pageable);

    List<DeviceData> findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
            String deviceKey, String identifier, Short deleted, LocalDateTime since,
            org.springframework.data.domain.Pageable pageable);

    // used by the diagnosis reflection retry to widen telemetry context beyond the latest-per-identifier snapshot
    List<DeviceData> findByDeviceKeyAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
            String deviceKey, Short deleted, LocalDateTime since, org.springframework.data.domain.Pageable pageable);
}
