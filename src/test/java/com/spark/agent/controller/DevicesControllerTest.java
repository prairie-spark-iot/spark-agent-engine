package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.dto.DeviceResponse;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.DeviceData;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DevicesControllerTest {

    @Mock
    private DeviceRepository deviceRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private DeviceDataRepository deviceDataRepository;

    @InjectMocks
    private DevicesController controller;

    @Test
    void list_mapsDeviceProductAndLatestTelemetry() {
        Device device = new Device();
        device.setDeviceKey("DK_INJ_001");
        device.setDeviceName("Injection Molder 01");
        device.setProductId(7L);
        device.setOnlineStatus((short) 1);

        Product product = new Product();
        product.setId(7L);
        product.setProductKey("PK_INJECTION_MA");

        DeviceData telemetry = new DeviceData();
        telemetry.setDeviceKey("DK_INJ_001");
        telemetry.setIdentifier("temperature");
        telemetry.setValue("235.5");

        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of(device));
        when(productRepository.findById(7L)).thenReturn(Optional.of(product));
        when(deviceDataRepository.findLatestByDeviceKey("DK_INJ_001")).thenReturn(List.of(telemetry));

        R<List<DeviceResponse>> result = controller.list();

        assertEquals(0, result.getCode());
        DeviceResponse response = result.getData().get(0);
        assertEquals("DK_INJ_001", response.deviceKey());
        assertEquals("Injection Molder 01", response.deviceName());
        assertEquals("PK_INJECTION_MA", response.productKey());
        assertTrue(response.online());
        assertEquals(1, response.telemetry().size());
        assertEquals("temperature", response.telemetry().get(0).identifier());
    }

    @Test
    void list_unknownProduct_productKeyIsNull() {
        Device device = new Device();
        device.setDeviceKey("DK_TEST");
        device.setDeviceName("Test Device");
        device.setProductId(999L);
        device.setOnlineStatus((short) 0);

        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of(device));
        when(productRepository.findById(999L)).thenReturn(Optional.empty());
        when(deviceDataRepository.findLatestByDeviceKey("DK_TEST")).thenReturn(List.of());

        R<List<DeviceResponse>> result = controller.list();

        DeviceResponse response = result.getData().get(0);
        assertNull(response.productKey());
        assertFalse(response.online());
        assertTrue(response.telemetry().isEmpty());
    }
}
