package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.dto.DeviceLatestResponse;
import com.spark.agent.dto.DeviceResponse;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * New plural /api/devices resource group for Phase 1/2 integration-contract endpoints — mirrors
 * AlertsController's split from the legacy singular /api/device/{key}/... in ApiController.
 * Gap-fills what was previously only exposed as the listDevices() MCP tool (not plain-fetchable
 * REST) — see spark-agent-docs/phase-1-2-api-data-contracts.md §2.
 */
@RestController
@RequestMapping("/api/devices")
@RequiredArgsConstructor
public class DevicesController {

    private final DeviceRepository deviceRepository;
    private final ProductRepository productRepository;
    private final DeviceDataRepository deviceDataRepository;

    @GetMapping
    public R<List<DeviceResponse>> list() {
        List<DeviceResponse> devices = deviceRepository.findByDeleted((short) 0).stream()
                .map(this::toResponse)
                .toList();
        return R.ok(devices);
    }

    private DeviceResponse toResponse(Device device) {
        String productKey = productRepository.findById(device.getProductId())
                .map(Product::getProductKey)
                .orElse(null);
        List<DeviceLatestResponse> telemetry = deviceDataRepository.findLatestByDeviceKey(device.getDeviceKey())
                .stream().map(DeviceLatestResponse::from).toList();
        return new DeviceResponse(
                device.getDeviceKey(),
                device.getDeviceName(),
                productKey,
                device.getOnlineStatus() == 1,
                telemetry);
    }
}
