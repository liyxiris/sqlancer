package sqlancer.hive.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema;
import sqlancer.hive.HiveSchema.HiveColumn;
import sqlancer.hive.HiveSchema.HiveLancerDataType;
import sqlancer.hive.ast.HiveColumnReference;
import sqlancer.hive.ast.HiveConstant;
import sqlancer.hive.ast.HiveExpression;
import sqlancer.postgres.ast.PostgresColumnValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HiveExpressionGenerator extends TypedExpressionGenerator<HiveExpression, HiveColumn, HiveLancerDataType> {

    private final int maxDepth;

    private final Randomly r;

    private HiveSchema.HiveRowValue rw;

    private final HiveGlobalState globalState;

    private final List<HiveColumnReference> columnRefs;

    // TODO
    public boolean allowAggregateFunctions;

    private enum Expression {
        // TODO: add or delete expressions.
        COLUMN, LITERAL, UNARY_PREFIX, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL,
        BINARY_COMPARISON, CAST, IN_OPERATION, BINARY_OPERATION, EXISTS, BETWEEN
    }

    public HiveExpressionGenerator(HiveGlobalState globalState) {
        this.globalState = globalState;
        this.columnRefs = new ArrayList<>();
    }

    public HiveExpression generateExpression(int depth, HiveLancerDataType originalType) {
        HiveLancerDataType dataType = originalType;
        // TODO: randomly cast some types like what PostgresExpressionGenerator does?
        return generateExpressionInternal(depth, dataType);
    }

    private HiveExpression generateExpressionInternal(int depth, HiveLancerDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
            allowAggregateFunctions = false; // aggregate function calls cannot be nested
            return getAggregate(dataType);
        }
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            // generic expression
            if (Randomly.getBoolean() || depth > maxDepth) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    return generateConstant(dataType);
                } else {
                    if (filterColumns(dataType).isEmpty()) {
                        return generateConstant(dataType);
                    } else {
                        return createColumnOfType(dataType);
                    }
                }
            }

        }
    }

    final List<HiveColumn> filterColumns(HiveLancerDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    private HiveExpression createColumnOfType(HiveLancerDataType type) {
        List<HiveColumn> columns = filterColumns(type);
        HiveColumn fromList = Randomly.fromList(columns);
        HiveConstant value = rw == null ? null : rw.getValues().get(fromList);
        return HiveColumnValue.create(fromList, value);
    }

    // TODO
    private HiveExpression getAggregate(HiveLancerDataType dataType) {

    }

    @Override
    public HiveExpression generatePredicate() {
        return null;
    }

    @Override
    public HiveExpression negatePredicate(HiveExpression predicate) {
        return null;
    }

    @Override
    public HiveExpression isNull(HiveExpression expr) {
        return null;
    }

    @Override
    public HiveExpression generateConstant(HiveLancerDataType type) {
        return null;
    }

    @Override
    protected HiveExpression generateExpression(HiveLancerDataType type, int depth) {
        return null;
    }

    @Override
    protected HiveExpression generateColumn(HiveLancerDataType type) {
        return null;
    }

    @Override
    protected HiveLancerDataType getRandomType() {
        return null;
    }

    @Override
    protected boolean canGenerateColumnOfType(HiveLancerDataType type) {
        return false;
    }

//    @Override
//    protected HiveExpression generateExpression(int depth) {
//
//    }
//
//    @Override
//    public HiveExpression generateConstant() {
//
//    }
//
//    @Override
//    protected HiveExpression generateColumn() {
//        HiveColumn c = Randomly.fromList(columns);
//        HiveConstant val;
//
//    }
//
//    @Override
//    public HiveExpression negatePredicate(HiveExpression predicate) {
//
//    }
//
//    @Override
//    public HiveExpression isNull(HiveExpression expr) {
//
//    }
//
//    @Override
//    public List<HiveExpression> generateOrderBys() {
//
//    }
}
