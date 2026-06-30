package com.spark.agent.repository;

import com.spark.agent.entity.Device;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DeviceRepository extends JpaRepository<Device, Long> {

    Optional<Device> findByDeviceKeyAndDeleted(String deviceKey, Short deleted);

    @Modifying
    @Query("UPDATE Device d SET d.onlineStatus = 1, d.lastOnlineTime = :time, d.updateTime = :time WHERE d.id = :id")
    void markOnline(Long id, LocalDateTime time);

    @Modifying
    @Query("UPDATE Device d SET d.onlineStatus = 0, d.lastOfflineTime = :time, d.updateTime = :time WHERE d.id IN :ids")
    void markOfflineBatch(List<Long> ids, LocalDateTime time);

    @Query("SELECT d FROM Device d WHERE d.onlineStatus = 1 AND d.deleted = 0")
    List<Device> findAllOnline();
}
