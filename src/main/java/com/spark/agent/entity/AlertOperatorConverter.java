package com.spark.agent.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Converter(autoApply = false)
public class AlertOperatorConverter implements AttributeConverter<AlertOperator, String> {

    @Override
    public String convertToDatabaseColumn(AlertOperator attribute) {
        return attribute == null ? null : attribute.code();
    }

    @Override
    public AlertOperator convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        AlertOperator op = AlertOperator.fromCode(dbData);
        if (op == null) {
            log.warn("[AlertRule] Unknown operator code '{}' in DB, treating as UNKNOWN (never matches)", dbData);
            return AlertOperator.UNKNOWN;
        }
        return op;
    }
}
