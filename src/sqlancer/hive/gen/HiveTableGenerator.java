package sqlancer.hive.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.hive.HiveErrors;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveProvider;
import sqlancer.hive.HiveSchema;

import java.util.ArrayList;
import java.util.List;

public class HiveTableGenerator {

    private enum HiveFileFormat {
        // JSONFILE
        SEQUENCEFILE, TEXTFILE, RCFILE, ORC, PARQUET, AVRO
    }

    public static SQLQueryAdapter generate(HiveGlobalState globalState, String tableName) {
        HiveFileFormat fileFormat = Randomly.fromOptions(HiveFileFormat.values());
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE ");
        sb.append("TABLE ");
        sb.append(globalState.getDatabaseName());
        sb.append(".");
        sb.append(tableName);
        sb.append(" (");

        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
//            String columnName = DBMSCommon.createColumnName(i);
            appendColumn(sb, i);
        }

        sb.append(") STORED AS ");
        sb.append(tableName);

        // TODO: CLUSTERED BY, SKEWED BY

        ExpectedErrors errors = new ExpectedErrors();
        HiveErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static void appendColumn(StringBuilder sb, int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        sb.append(columnName);
        sb.append(" ");
        sb.append(HiveSchema.HiveLancerDataType.getRandom());
    }
}
