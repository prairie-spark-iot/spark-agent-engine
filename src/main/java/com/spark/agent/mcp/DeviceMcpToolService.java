package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceStatusResult;
import com.spark.agent.entity.AlertRecord;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.repository.VectorStoreRepository;
import com.spark.agent.service.RagSearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceMcpToolService {

    private final AlertRecordRepository alertRecordRepository;
    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final RagSearchService ragSearchService;

    @Tool(description = "Query a device's online status and latest telemetry value per identifier, by device key")
    public DeviceStatusResult queryDeviceStatus(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey) {
        log.debug("[MCP Tool] queryDeviceStatus deviceKey={}", deviceKey);
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
        log.debug("[MCP Tool] queryDeviceHistory deviceKey={} identifier={} hours={}", deviceKey, identifier, hours);
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        return deviceDataRepository.findByDeviceKeyAndIdentifierAndDeletedAndReportTimeGreaterThanEqualOrderByReportTimeDesc(
                deviceKey, identifier, (short) 0, since, PageRequest.of(0, 500));
    }

    @Tool(description = "Query recent alert records for a device, newest first")
    public List<AlertRecord> queryDeviceAlerts(
            @ToolParam(description = "the device's unique key, e.g. DK_INJ_001") String deviceKey,
            @ToolParam(description = "max number of alerts to return; defaults to 20 if omitted or <= 0, capped at 500", required = false) int limit) {
        log.debug("[MCP Tool] queryDeviceAlerts deviceKey={} limit={}", deviceKey, limit);
        int effectiveLimit = Math.min(limit > 0 ? limit : 20, 500);
        return alertRecordRepository.findByDeviceKeyAndDeletedOrderByTriggerTimeDesc(
                deviceKey, (short) 0, PageRequest.of(0, effectiveLimit));
    }

    @Tool(description = "Search the device manual/knowledge base for a device model and return relevant excerpts")
    public List<VectorStoreRepository.SearchResult> queryDeviceManual(
            @ToolParam(description = "the device's product model/key, e.g. PK_INJECTION_MA — this is the product_key, not the device's display name; get it from queryDeviceStatus if unknown") String deviceModel,
            @ToolParam(description = "the question or symptom to search the manual for") String question) {
        log.debug("[MCP Tool] queryDeviceManual deviceModel={} question={}", deviceModel, question);
        return ragSearchService.search(question, deviceModel, 5);
    }
}
