# AlertOperator Enum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the string-typed `AlertRule.operator` field with a strongly-typed `AlertOperator` enum, so illegal operator values are caught at compile time and `AlertService` no longer does runtime string comparison to evaluate a rule.

**Architecture:** New `AlertOperator` enum (one constant per operator, each implementing its own `matches(value, threshold)` comparison) plus a JPA `AttributeConverter` that maps the enum to/from the existing lowercase `varchar` DB codes, defaulting unrecognized DB values to a safe `UNKNOWN` constant instead of throwing. `AlertRule.operator` changes type from `String` to `AlertOperator`; `AlertService.matches()` delegates to the enum instead of a `switch`.

**Tech Stack:** Java 25, Spring Boot 4.1.0 (Jakarta Persistence), JUnit 5, Mockito.

## Global Constraints

- No DB schema change — `aiot_alert_rule.operator` stays `varchar`, storing the same lowercase codes ("gt", "lt", "gte", "lte", "eq", "ne").
- Alert content text emitted by `AlertService.buildRecord()` must stay byte-identical to today (uses the operator's `code()`, not its enum name).
- Unrecognized DB operator values map to `AlertOperator.UNKNOWN` (which never matches) with a logged warning — never throw during entity hydration.
- Epsilon for EQ/NE comparisons stays `1e-10`, matching current behavior.

---

### Task 1: Create AlertOperator enum and AlertOperatorConverter with unit tests

**Files:**
- Create: `src/main/java/com/spark/agent/entity/AlertOperator.java`
- Create: `src/main/java/com/spark/agent/entity/AlertOperatorConverter.java`
- Create: `src/test/java/com/spark/agent/entity/AlertOperatorTest.java`
- Create: `src/test/java/com/spark/agent/entity/AlertOperatorConverterTest.java`

**Interfaces:**
- Produces: `AlertOperator` enum with constants `GT, LT, GTE, LTE, EQ, NE, UNKNOWN`, instance methods `boolean matches(double value, double threshold)` and `String code()`, static `AlertOperator fromCode(String code)` (returns `null` if unrecognized) — consumed by Task 2.
- Produces: `AlertOperatorConverter implements AttributeConverter<AlertOperator, String>` — consumed by Task 2.

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/com/spark/agent/entity/AlertOperatorTest.java`:

```java
package com.spark.agent.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AlertOperatorTest {

    @Test
    void gt_matchesWhenAboveThreshold() {
        assertTrue(AlertOperator.GT.matches(101, 100));
        assertFalse(AlertOperator.GT.matches(100, 100));
        assertFalse(AlertOperator.GT.matches(99, 100));
    }

    @Test
    void lt_matchesWhenBelowThreshold() {
        assertTrue(AlertOperator.LT.matches(99, 100));
        assertFalse(AlertOperator.LT.matches(100, 100));
        assertFalse(AlertOperator.LT.matches(101, 100));
    }

    @Test
    void gte_matchesWhenAboveOrEqualThreshold() {
        assertTrue(AlertOperator.GTE.matches(101, 100));
        assertTrue(AlertOperator.GTE.matches(100, 100));
        assertFalse(AlertOperator.GTE.matches(99, 100));
    }

    @Test
    void lte_matchesWhenBelowOrEqualThreshold() {
        assertTrue(AlertOperator.LTE.matches(99, 100));
        assertTrue(AlertOperator.LTE.matches(100, 100));
        assertFalse(AlertOperator.LTE.matches(101, 100));
    }

    @Test
    void eq_matchesWithinEpsilon() {
        assertTrue(AlertOperator.EQ.matches(99.9, 99.9));
        assertFalse(AlertOperator.EQ.matches(99.9001, 99.9));
    }

    @Test
    void ne_matchesOutsideEpsilon() {
        assertTrue(AlertOperator.NE.matches(50, 100));
        assertFalse(AlertOperator.NE.matches(99.9, 99.9));
    }

    @Test
    void unknown_neverMatches() {
        assertFalse(AlertOperator.UNKNOWN.matches(100, 100));
        assertFalse(AlertOperator.UNKNOWN.matches(0, 0));
    }

    @Test
    void code_returnsDbLowercaseCode() {
        assertEquals("gt", AlertOperator.GT.code());
        assertEquals("lt", AlertOperator.LT.code());
        assertEquals("gte", AlertOperator.GTE.code());
        assertEquals("lte", AlertOperator.LTE.code());
        assertEquals("eq", AlertOperator.EQ.code());
        assertEquals("ne", AlertOperator.NE.code());
    }

    @Test
    void fromCode_returnsMatchingConstantForValidCodes() {
        assertEquals(AlertOperator.GT, AlertOperator.fromCode("gt"));
        assertEquals(AlertOperator.LT, AlertOperator.fromCode("lt"));
        assertEquals(AlertOperator.GTE, AlertOperator.fromCode("gte"));
        assertEquals(AlertOperator.LTE, AlertOperator.fromCode("lte"));
        assertEquals(AlertOperator.EQ, AlertOperator.fromCode("eq"));
        assertEquals(AlertOperator.NE, AlertOperator.fromCode("ne"));
    }

    @Test
    void fromCode_returnsNullForUnrecognizedCode() {
        assertNull(AlertOperator.fromCode("invalid_op"));
    }
}
```

Create `src/test/java/com/spark/agent/entity/AlertOperatorConverterTest.java`:

```java
package com.spark.agent.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AlertOperatorConverterTest {

    private final AlertOperatorConverter converter = new AlertOperatorConverter();

    @Test
    void convertToDatabaseColumn_returnsCode() {
        assertEquals("gt", converter.convertToDatabaseColumn(AlertOperator.GT));
        assertEquals("ne", converter.convertToDatabaseColumn(AlertOperator.NE));
    }

    @Test
    void convertToDatabaseColumn_nullAttributeReturnsNull() {
        assertNull(converter.convertToDatabaseColumn(null));
    }

    @Test
    void convertToEntityAttribute_mapsKnownCodeToConstant() {
        assertEquals(AlertOperator.GT, converter.convertToEntityAttribute("gt"));
        assertEquals(AlertOperator.LTE, converter.convertToEntityAttribute("lte"));
    }

    @Test
    void convertToEntityAttribute_unknownCodeMapsToUnknownConstant() {
        assertEquals(AlertOperator.UNKNOWN, converter.convertToEntityAttribute("invalid_op"));
    }

    @Test
    void convertToEntityAttribute_nullDbDataReturnsNull() {
        assertNull(converter.convertToEntityAttribute(null));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.entity.AlertOperatorTest" --tests "com.spark.agent.entity.AlertOperatorConverterTest"`
Expected: FAIL — compile error, `AlertOperator` and `AlertOperatorConverter` do not exist yet.

- [ ] **Step 3: Write the AlertOperator enum**

Create `src/main/java/com/spark/agent/entity/AlertOperator.java`:

```java
package com.spark.agent.entity;

public enum AlertOperator {
    GT("gt") {
        @Override
        public boolean matches(double value, double threshold) {
            return value > threshold;
        }
    },
    LT("lt") {
        @Override
        public boolean matches(double value, double threshold) {
            return value < threshold;
        }
    },
    GTE("gte") {
        @Override
        public boolean matches(double value, double threshold) {
            return value >= threshold;
        }
    },
    LTE("lte") {
        @Override
        public boolean matches(double value, double threshold) {
            return value <= threshold;
        }
    },
    EQ("eq") {
        @Override
        public boolean matches(double value, double threshold) {
            return Math.abs(value - threshold) < EPSILON;
        }
    },
    NE("ne") {
        @Override
        public boolean matches(double value, double threshold) {
            return Math.abs(value - threshold) >= EPSILON;
        }
    },
    UNKNOWN("unknown") {
        @Override
        public boolean matches(double value, double threshold) {
            return false;
        }
    };

    private static final double EPSILON = 1e-10;

    private final String code;

    AlertOperator(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public abstract boolean matches(double value, double threshold);

    public static AlertOperator fromCode(String code) {
        for (AlertOperator op : values()) {
            if (op.code.equals(code)) {
                return op;
            }
        }
        return null;
    }
}
```

- [ ] **Step 4: Write the AlertOperatorConverter**

Create `src/main/java/com/spark/agent/entity/AlertOperatorConverter.java`:

```java
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.entity.AlertOperatorTest" --tests "com.spark.agent.entity.AlertOperatorConverterTest"`
Expected: PASS (9 + 5 tests)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/spark/agent/entity/AlertOperator.java src/main/java/com/spark/agent/entity/AlertOperatorConverter.java src/test/java/com/spark/agent/entity/AlertOperatorTest.java src/test/java/com/spark/agent/entity/AlertOperatorConverterTest.java
git commit -m "feat(alert): add AlertOperator enum and JPA converter"
```

---

### Task 2: Wire AlertOperator into AlertRule and AlertService

**Files:**
- Modify: `src/main/java/com/spark/agent/entity/AlertRule.java:29-31`
- Modify: `src/main/java/com/spark/agent/service/AlertService.java:96-116,132-134`
- Modify: `src/test/java/com/spark/agent/service/AlertServiceTest.java`

**Interfaces:**
- Consumes: `AlertOperator` enum and `AlertOperatorConverter` from Task 1.

- [ ] **Step 1: Update AlertServiceTest to use AlertOperator**

In `AlertServiceTest.java`, add the import:

```java
import com.spark.agent.entity.AlertOperator;
```

Change `setUp()`'s rule construction:

```java
        sampleRule = new AlertRule();
        sampleRule.setId(100L);
        sampleRule.setName("High Temperature");
        sampleRule.setOperator(AlertOperator.GT);
        sampleRule.setThreshold("100");
        sampleRule.setLevel((short) 2);
```

Change the operator-specific tests to pass `AlertOperator` constants instead of strings:

```java
    @Test
    void evaluate_gtOperator_triggersWhenAbove() {
        sampleData.setValueNum(new BigDecimal("101"));
        testOperatorTriggers(AlertOperator.GT, "100");
    }

    @Test
    void evaluate_ltOperator_triggersWhenBelow() {
        sampleData.setValueNum(new BigDecimal("50"));
        testOperatorTriggers(AlertOperator.LT, "100");
    }

    @Test
    void evaluate_gteOperator_triggersWhenEqual() {
        sampleData.setValueNum(new BigDecimal("100"));
        testOperatorTriggers(AlertOperator.GTE, "100");
    }

    @Test
    void evaluate_lteOperator_triggersWhenEqual() {
        sampleData.setValueNum(new BigDecimal("100"));
        testOperatorTriggers(AlertOperator.LTE, "100");
    }

    @Test
    void evaluate_eqOperator_triggersOnExactMatch() {
        sampleData.setValueNum(new BigDecimal("99.9"));
        testOperatorTriggers(AlertOperator.EQ, "99.9");
    }

    @Test
    void evaluate_eqOperator_doesNotTriggerOnNearMatch() {
        sampleData.setValueNum(new BigDecimal("99.9001"));
        sampleRule.setOperator(AlertOperator.EQ);
        sampleRule.setThreshold("99.9");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void evaluate_neOperator_triggersOnDifferent() {
        sampleData.setValueNum(new BigDecimal("50"));
        testOperatorTriggers(AlertOperator.NE, "100");
    }

    @Test
    void evaluate_invalidThreshold_doesNotTrigger() {
        sampleRule.setThreshold("not-a-number");
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    @Test
    void evaluate_unknownOperator_doesNotTrigger() {
        sampleRule.setOperator(AlertOperator.UNKNOWN);
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));

        alertService.evaluate(sampleData);

        verify(alertRecordRepository, never()).save(any());
    }

    private void testOperatorTriggers(AlertOperator operator, String threshold) {
        sampleRule.setOperator(operator);
        sampleRule.setThreshold(threshold);
        when(alertRuleRepository.findActiveRules(1L, "temperature")).thenReturn(List.of(sampleRule));
        when(alertRecordRepository.countRecentUnhandled(eq(1L), eq(100L), any(LocalDateTime.class)))
                .thenReturn(0L);
        when(idGenerator.nextId()).thenReturn(999L);
        OutboxMessage outboxMessage = new OutboxMessage();
        when(outboxMessageFactory.build(eq("alert_record"), eq("999"), eq("alert.triggered"), any(AlertRecord.class)))
                .thenReturn(outboxMessage);

        alertService.evaluate(sampleData);

        verify(alertRecordRepository).save(any(AlertRecord.class));
        verify(outboxMessageRepository).save(same(outboxMessage));
    }
```

(`evaluate_invalidOperator_doesNotTrigger` is renamed `evaluate_unknownOperator_doesNotTrigger` and now sets `AlertOperator.UNKNOWN` directly, since an arbitrary invalid string can no longer be assigned to the field — this is the compile-time safety the task is about. All other test methods not listed above are unchanged.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest"`
Expected: FAIL — compile error, `AlertRule.setOperator(AlertOperator)` does not exist yet (field is still `String`).

- [ ] **Step 3: Update AlertRule**

In `AlertRule.java`, replace:

```java
    /** gt / lt / gte / lte / eq / ne */
    @Column(name = "operator", nullable = false)
    private String operator;
```

with:

```java
    /** gt / lt / gte / lte / eq / ne */
    @Column(name = "operator", nullable = false)
    @Convert(converter = AlertOperatorConverter.class)
    private AlertOperator operator;
```

Add the import (alongside the existing `jakarta.persistence.*` wildcard import, no new import line is needed since `@Convert` is part of `jakarta.persistence.*`; `AlertOperatorConverter` is in the same package `com.spark.agent.entity` so it needs no import either).

- [ ] **Step 4: Update AlertService**

In `AlertService.java`, replace `matches()`:

```java
    private boolean matches(AlertRule rule, double value) {
        try {
            double threshold = Double.parseDouble(rule.getThreshold());
            return rule.getOperator().matches(value, threshold);
        } catch (NumberFormatException e) {
            log.warn("[Alert] Unparseable threshold '{}' for rule {}", rule.getThreshold(), rule.getId());
            return false;
        }
    }
```

And in `buildRecord()`, change the format call to use `rule.getOperator().code()`:

```java
        r.setAlertContent(String.format("设备 %s 属性 %s 当前值 %s 触发规则「%s」(阈值: %s %s)",
                data.getDeviceKey(), data.getIdentifier(), data.getValue(),
                rule.getName(), rule.getOperator().code(), rule.getThreshold()));
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew test --tests "com.spark.agent.service.AlertServiceTest" --tests "com.spark.agent.entity.AlertOperatorTest" --tests "com.spark.agent.entity.AlertOperatorConverterTest"`
Expected: PASS (all tests in all three classes)

- [ ] **Step 6: Run the full build**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL (confirms no other file references `AlertRule.getOperator()`/`setOperator()` as a `String`)

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/spark/agent/entity/AlertRule.java src/main/java/com/spark/agent/service/AlertService.java src/test/java/com/spark/agent/service/AlertServiceTest.java
git commit -m "refactor(alert): use AlertOperator enum instead of raw operator strings"
```
