package sqlancer.hive.ast;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public abstract class HiveConstant extends HiveExpression {

    public static class IntConstant extends HiveConstant {

        private final long value;

        public IntConstant(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class FloatConstant extends HiveConstant {

        private final float value;

        public FloatConstant(float value) {
            this.value = value;
        }

        public float getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Float.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Float.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }
    }

    public static class DoubleConstant extends HiveConstant {

        private final double value;

        public DoubleConstant(float value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }
    }

    public static class DecimalConstant extends HiveConstant {

        private final BigDecimal value;

        public DecimalConstant(BigDecimal value) {
            this.value = value;
        }

        public BigDecimal getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class TimestampConstant extends HiveConstant {

        private final String textRepr;

        public TimestampConstant(long value) {
            Timestamp timestamp = new Timestamp(value);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            this.textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }
    }

    public static class DateConstant extends HiveConstant {

        private final String textRepr;

        public DateConstant(long value) {
            Timestamp timestamp = new Timestamp(value);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            this.textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }
    }

    public static class StringConstant extends HiveConstant {

        private final String value;

        public StringConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }
    }

    public static class BooleanConstant extends HiveConstant {

        private final boolean value;

        public BooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}
