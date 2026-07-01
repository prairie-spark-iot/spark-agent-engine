package com.spark.agent.service;

import com.spark.agent.dto.DiagnosisResult;
import com.spark.agent.repository.AlertRecordRepository;
import com.spark.agent.repository.DeviceDataRepository;
import com.spark.agent.repository.DeviceRepository;
import com.spark.agent.repository.VectorStoreRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisAgentService {

    private final DeviceRepository deviceRepository;
    private final DeviceDataRepository deviceDataRepository;
    private final AlertRecordRepository alertRecordRepository;
    private final VectorStoreRepository vectorStoreRepository;

    public DiagnosisResult diagnose(String alertMessage) {
        log.info("[Diagnosis] diagnose() stub invoked, alertMessage={}", alertMessage);
        return null;
    }
}
