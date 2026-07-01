package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceStatusResult;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DeviceMcpToolService {

    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;

    @Tool(description = "Query a device's online status and latest telemetry value per identifier, by device key")
    public DeviceStatusResult queryDeviceStatus(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey) {
        Device device = deviceRepository.findByDeviceKeyAndDeleted(deviceKey, (short) 0).orElse(null);
        if (device == null) {
            return DeviceStatusResult.notFound(deviceKey);
        }
        String productKey = productRepository.findById(device.getProductId())
                .map(Product::getProductKey)
                .orElse(null);
        return new DeviceStatusResult(
                deviceKey,
                true,
                device.getDeviceName(),
                productKey,
                device.getOnlineStatus() == 1,
                device.getLastOnlineTime(),
                device.getLastOfflineTime(),
                deviceDataRepository.findLatestByDeviceKey(deviceKey));
    }

    @Tool(description = "Query historical telemetry values for one identifier of a device over the last N hours, newest first (capped at 500 rows)")
    public List<DeviceData> queryDeviceHistory(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "the telemetry identifier, e.g. temperature, pressure, current") String identifier,
            @ToolParam(description = "how many hours of history to look back from now") int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        return deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                deviceKey, identifier, (short) 0, since, PageRequest.of(0, 500));
    }
}
