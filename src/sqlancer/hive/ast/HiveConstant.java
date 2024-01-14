package sqlancer.hive.ast;

public abstract class HiveConstant extends HiveExpression {

    public abstract boolean isNull();

    public static class HiveIntConstant extends HiveConstant {

        private final long value;

        @Override
        public boolean isNull() {
            return false;
        }
    }
}
