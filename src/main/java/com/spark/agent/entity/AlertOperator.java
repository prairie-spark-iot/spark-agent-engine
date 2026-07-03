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
