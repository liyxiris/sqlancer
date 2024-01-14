package sqlancer.hive.oracle;

import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hive.HiveErrors;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema;
import sqlancer.hive.ast.HiveExpression;
import sqlancer.hive.ast.HiveSelect;
import sqlancer.hive.gen.HiveExpressionGenerator;

public class HiveTLPBase extends TernaryLogicPartitioningOracleBase<HiveExpression, HiveGlobalState>
    implements TestOracle<HiveGlobalState> {

    HiveSchema schema;
    HiveExpressionGenerator gen;
    HiveSelect select;

    public HiveTLPBase(HiveGlobalState state) {
        super(state);
        HiveErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        gen = new HiveExpressionGenerator(state);
        schema = state.getSchema();
        select = new HiveSelect();

    }

    @Override
    protected ExpressionGenerator<HiveExpression> getGen() {
        return gen;
    }
}
