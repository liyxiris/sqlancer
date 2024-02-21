package sqlancer.hive.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema.*;
import sqlancer.hive.ast.HiveColumnReference;
import sqlancer.hive.ast.HiveConstant;
import sqlancer.hive.ast.HiveExpression;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HiveExpressionGenerator extends UntypedExpressionGenerator<Node<HiveExpression>, HiveColumn> {

    private final int maxDepth;

    private final Randomly r;

    private HiveRowValue rw;

    private final HiveGlobalState globalState;

    private final List<HiveColumnReference> columnRefs;

    // TODO
    public boolean allowAggregateFunctions;

    private enum Expression {
        // TODO: add or delete expressions.
        UNARY_PREFIX, UNARY_POSTFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC,
        CAST, FUNC, BETWEEN, IN, CASE, BINARY_OPERATION, EXISTS,

    }

    public HiveExpressionGenerator(HiveGlobalState globalState) {
        this.globalState = globalState;
        this.columnRefs = new ArrayList<>();
    }

    final List<HiveColumn> filterColumns(HiveDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    private HiveExpression createColumnOfType(HiveDataType type) {
        List<HiveColumn> columns = filterColumns(type);
        HiveColumn fromList = Randomly.fromList(columns);
        HiveConstant value = rw == null ? null : rw.getValues().get(fromList);
        return HiveColumnValue.create(fromList, value);
    }

    @Override
    public Node<HiveExpression> generatePredicate() {
        return null;
    }

    @Override
    public Node<HiveExpression> negatePredicate(Node<HiveExpression> predicate) {
        return null;
    }

    @Override
    public Node<HiveExpression> isNull(Node<HiveExpression> expr) {
        return null;
    }

    @Override
    public Node<HiveExpression> generateConstant() {
        return null;
    }

    @Override
    protected Node<HiveExpression> generateExpression(int depth) {
        // TODO: randomly cast some types like what PostgresExpressionGenerator does?
        return generateExpressionInternal(depth, dataType);
    }

    private Node<HiveExpression> generateExpressionInternal(int depth, HiveDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBooleanWithRatherLowProbability()) {
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
            } else {

            }

        }

        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        // TODO: remove some of the possible expression types according to options.

        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
            case UNARY_PREFIX:
                return new NewUnaryPrefixOperatorNode<>(generateExpression(depth + 1),
                        HiveUnaryPrefixOperator.getRandom());
            case UNARY_POSTFIX:
                return new NewUnaryPostfixOperatorNode<>(generateExpression(depth + 1),
                        HiveExpressionGenerator.HiveUnaryPostfixOperator.getRandom());
            case BINARY_COMPARISON:
                Operator op = HiveBinaryComparisonOperator.getRandom();
                return new NewBinaryOperatorNode<>(generateExpression(depth + 1),
                        generateExpression(depth + 1), op);
            case BINARY_LOGICAL:
                op = HiveExpressionGenerator.HiveBinaryLogicalOperator.getRandom();
                return new NewBinaryOperatorNode<>(generateExpression(depth + 1),
                        generateExpression(depth + 1), op);
            case BINARY_ARITHMETIC:
                return new NewBinaryOperatorNode<>(generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        HiveExpressionGenerator.HiveBinaryArithmeticOperator.getRandom());
            case CAST:
                return new HiveCastOperation(generateExpression(depth + 1),
                        HiveSchema.HiveCompositeDataType.getRandomWithoutNull());
            case FUNC:
                HiveFunction func = HiveFunction.getRandom();
                return new NewFunctionNode<>(generateExpressions(func.getNrArgs()), func);
            case BETWEEN:
                return new NewBetweenOperatorNode<>(generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        Randomly.getBoolean());
            case IN:
                return new NewInOperatorNode<>(generateExpression(depth + 1),
                        generateExpressions(Randomly.smallNumber() + 1, depth + 1),
                        Randomly.getBoolean());
            case CASE:
                int nr = Randomly.smallNumber() + 1;
                return new NewCaseOperatorNode<>(generateExpression(depth + 1),
                        generateExpressions(nr, depth + 1),
                        generateExpressions(nr, depth + 1),
                        generateExpression(depth + 1));
            default:
                throw new AssertionError();
        }
    }

    // TODO
    private HiveExpression getAggregate(HiveDataType dataType) {

    }

    @Override
    protected Node<HiveExpression> generateColumn() {
        return null;
    }

    public enum HiveUnaryPrefixOperator implements Operator {

        // TODO: ~A (bitwise NOT)
        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        HiveUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveUnaryPostfixOperator implements Operator {

        // TODO: A IS [NOT] (NULL|TRUE|FALSE)...
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        HiveUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveBinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"),
        SMALLER_EQUALS("<="), NOT_EQUALS("!="), LIKE("LIKE"),
        NOT_LIKE("NOT LIKE"), REGEXP("RLIKE");

        private String textRepr;

        HiveBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveBinaryLogicalOperator implements Operator {

        AND("AND"), OR("OR");

        private String textRepr;

        HiveBinaryLogicalOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public enum HiveBinaryArithmeticOperator implements Operator {

        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), BITWISE_AND("&"), BITWISE_OR("|"),
        BITWISE_XOR("^");

        private String textRepr;

        HiveBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    // TODO: test all Hive default functions...
    public enum HiveFunction {
        // trigonometric functions
        ACOS(1), //
        ASIN(1), //
        ATAN(1), //
        COS(1), //
        SIN(1), //
        TAN(1), //
        COT(1), //
        ATAN2(1), //
        // math functions
        ABS(1), //
        CEIL(1), //
        CEILING(1), //
        FLOOR(1), //
        LOG(1), //
        LOG10(1), LOG2(1), //
        LN(1), //
        PI(0), //
        SQRT(1), //
        POWER(1), //
        CBRT(1), //
        ROUND(2), //
        SIGN(1), //
        DEGREES(1), //
        RADIANS(1), //
        MOD(2), //
        XOR(2), //
        // string functions
        LENGTH(1), //
        LOWER(1), //
        UPPER(1), //
        SUBSTRING(3), //
        REVERSE(1), //
        CONCAT(1, true), //
        CONCAT_WS(1, true), CONTAINS(2), //
        PREFIX(2), //
        SUFFIX(2), //
        INSTR(2), //
        PRINTF(1, true), //
        REGEXP_MATCHES(2), //
        REGEXP_REPLACE(3), //
        STRIP_ACCENTS(1), //

        // date functions
        DATE_PART(2), AGE(2),

        COALESCE(3), NULLIF(2),

        // LPAD(3),
        // RPAD(3),
        LTRIM(1), RTRIM(1),
        // LEFT(2), https://github.com/cwida/duckdb/issues/633
        // REPEAT(2),
        REPLACE(3), UNICODE(1),

        BIT_COUNT(1), BIT_LENGTH(1), LAST_DAY(1), MONTHNAME(1), DAYNAME(1), YEARWEEK(1), DAYOFMONTH(1), WEEKDAY(1),
        WEEKOFYEAR(1),

        IFNULL(2), IF(3);

        private int nrArgs;
        private boolean isVariadic;

        HiveFunction(int nrArgs) {
            this(nrArgs, false);
        }

        HiveFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static HiveFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }

    }
}
