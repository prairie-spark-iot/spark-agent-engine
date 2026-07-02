package com.spark.agent.mcp;

import com.spark.agent.dto.DeviceSummary;
import com.spark.agent.entity.Device;
import com.spark.agent.entity.Product;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.ProductRepository;
import com.spark.agent.service.RagSearchService;
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
class DeviceMcpToolServiceTest {

    @Mock
    private AlertRecordRepository alertRecordRepository;
    @Mock
    private DeviceRepository deviceRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private DeviceDataRepository deviceDataRepository;
    @Mock
    private RagSearchService ragSearchService;

    @InjectMocks
    private DeviceMcpToolService service;

    @Test
    void listDevices_mapsDeviceAndProductFields() {
        Device device = new Device();
        device.setDeviceKey("DK_INJ_002");
        device.setDeviceName("injection_02");
        device.setProductId(50L);
        device.setOnlineStatus((short) 1);

        Product product = new Product();
        product.setProductKey("PK_INJECTION_MA");

        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of(device));
        when(productRepository.findById(50L)).thenReturn(Optional.of(product));

        List<DeviceSummary> result = service.listDevices();

        assertEquals(1, result.size());
        DeviceSummary summary = result.get(0);
        assertEquals("DK_INJ_002", summary.deviceKey());
        assertEquals("injection_02", summary.deviceName());
        assertEquals("PK_INJECTION_MA", summary.productKey());
        assertTrue(summary.online());
    }

    @Test
    void listDevices_offlineDevice_reportsOnlineFalse() {
        Device device = new Device();
        device.setDeviceKey("DK_CMP_001");
        device.setDeviceName("compressor_01");
        device.setProductId(51L);
        device.setOnlineStatus((short) 0);

        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of(device));
        when(productRepository.findById(51L)).thenReturn(Optional.empty());

        List<DeviceSummary> result = service.listDevices();

        assertEquals(1, result.size());
        assertFalse(result.get(0).online());
        assertNull(result.get(0).productKey());
    }

    @Test
    void listDevices_noDevices_returnsEmptyList() {
        when(deviceRepository.findByDeleted((short) 0)).thenReturn(List.of());

        assertTrue(service.listDevices().isEmpty());
    }
}
