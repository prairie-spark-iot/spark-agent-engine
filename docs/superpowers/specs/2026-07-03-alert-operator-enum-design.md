# AlertOperator Enum (P3 task 021)

## Goal

Replace the string-typed `AlertRule.operator` field ("gt", "lt", "gte", "lte", "eq", "ne")
with a strongly-typed `AlertOperator` enum, so illegal operator values are caught at compile
time and `AlertService` no longer does runtime string comparison to decide how to evaluate a
rule.

## Non-goals

- No DB schema change. `aiot_alert_rule.operator` stays `varchar`, storing the same lowercase
  codes ("gt", "lt", ...) — the enum maps to/from that column via a converter.
- No change to alert content text emitted today — the format string continues to produce
  the exact same string it does now.
- No REST API for creating/editing alert rules exists in this codebase; this change only
  affects how `AlertRule` is read and evaluated.

## New enum: `com.spark.agent.entity.AlertOperator`

```java
public enum AlertOperator {
    GT("gt")   { public boolean matches(double value, double threshold) { return value > threshold; } },
    LT("lt")   { public boolean matches(double value, double threshold) { return value < threshold; } },
    GTE("gte") { public boolean matches(double value, double threshold) { return value >= threshold; } },
    LTE("lte") { public boolean matches(double value, double threshold) { return value <= threshold; } },
    EQ("eq")   { public boolean matches(double value, double threshold) { return Math.abs(value - threshold) < EPSILON; } },
    NE("ne")   { public boolean matches(double value, double threshold) { return Math.abs(value - threshold) >= EPSILON; } },
    UNKNOWN("unknown") { public boolean matches(double value, double threshold) { return false; } };

    private static final double EPSILON = 1e-10;

    private final String code;

    AlertOperator(String code) { this.code = code; }

    public String code() { return code; }

    public abstract boolean matches(double value, double threshold);

    public static AlertOperator fromCode(String code) {
        for (AlertOperator op : values()) {
            if (op.code.equals(code)) return op;
        }
        return null;
    }
}
```

`fromCode` returns `null` for unrecognized input rather than `UNKNOWN` directly — `UNKNOWN` is
reserved for "the converter saw bad data and needs a safe non-null value"; `fromCode` itself
stays a pure lookup with no logging side effect.

## New converter: `com.spark.agent.entity.AlertOperatorConverter`

```java
@Slf4j
@Converter(autoApply = false)
public class AlertOperatorConverter implements AttributeConverter<AlertOperator, String> {

    @Override
    public String convertToDatabaseColumn(AlertOperator attribute) {
        return attribute == null ? null : attribute.code();
    }

    @Override
    public AlertOperator convertToEntityAttribute(String dbData) {
        if (dbData == null) return null;
        AlertOperator op = AlertOperator.fromCode(dbData);
        if (op == null) {
            log.warn("[AlertRule] Unknown operator code '{}' in DB, treating as UNKNOWN (never matches)", dbData);
            return AlertOperator.UNKNOWN;
        }
        return op;
    }
}
```

Mapping unrecognized DB values to `UNKNOWN` (rather than throwing) preserves today's per-rule
resilience: one row with bad legacy data logs a warning and never matches, instead of failing
the whole `findActiveRules()` result set.

## AlertRule changes

```java
/** gt / lt / gte / lte / eq / ne */
@Column(name = "operator", nullable = false)
@Convert(converter = AlertOperatorConverter.class)
private AlertOperator operator;
```

(Field type changes from `String` to `AlertOperator`; `@Convert` replaces the plain
`@Column` string mapping.)

## AlertService changes

`matches()` simplifies to:

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

The `switch` over operator strings and the "Unknown operator" log branch are removed —
`UNKNOWN.matches()` already returns `false`, and the converter already logged the anomaly at
load time.

`buildRecord()`'s alert-content format string uses `rule.getOperator().code()` instead of the
raw field, so the emitted string is byte-identical to today (e.g. `"...阈值: gt 100.0)"`, not
`"...阈值: GT 100.0)"`).

## Testing

- New `AlertOperatorTest`: `matches()` for each constant (including epsilon edge cases for
  EQ/NE, mirroring the existing `AlertServiceTest` operator cases), `fromCode()` for all valid
  codes and an invalid code (returns `null`), `code()` round-trips.
- New `AlertOperatorConverterTest`: `convertToDatabaseColumn` returns the code for each
  constant; `convertToEntityAttribute` maps known codes back to the right constant, maps an
  unrecognized code to `UNKNOWN`, and maps `null` to `null`.
- `AlertServiceTest`: replace all `sampleRule.setOperator("gt")`-style calls with
  `sampleRule.setOperator(AlertOperator.GT)` etc.; update the `testOperatorTriggers` helper to
  take an `AlertOperator` parameter instead of `String`. Repurpose
  `evaluate_invalidOperator_doesNotTrigger` to call `sampleRule.setOperator(AlertOperator.UNKNOWN)`
  directly (simulating what the converter produces for bad legacy data) and assert no alert
  triggers — preserving coverage of the resilience path in a way that's still compile-time
  valid.

## Acceptance criteria

Illegal operator values are caught at compile time (the `AlertRule.operator` setter no longer
accepts arbitrary strings); `AlertService` contains no runtime string comparison to decide how
to evaluate a rule.
