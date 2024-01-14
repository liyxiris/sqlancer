package sqlancer.hive;

import com.clickhouse.client.ClickHouseDataType;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import org.apache.hadoop.hive.serde2.thrift.Type;
import sqlancer.hive.ast.HiveConstant;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveSchema extends AbstractSchema<HiveGlobalState, HiveSchema.HiveTable> {

    public static class HiveLancerDataType {

        private final Type hiveType;
        private final String typeName;

        public HiveLancerDataType(Type type) {
            this.hiveType = type;
            this.typeName = type.getName();
        }

        public static HiveLancerDataType getRandom() {
            return new HiveLancerDataType(
                    Randomly.fromOptions(Type.INT_TYPE, Type.STRING_TYPE));
        }

        public Type getType() {
            return hiveType;
        }

        @Override
        public String toString() {
            return typeName;
        }
    }

    public static class HiveColumn extends AbstractTableColumn<HiveTable, HiveLancerDataType> {

        public HiveColumn(String name, HiveTable table, HiveLancerDataType type) {
            super(name, table, type);
        }
    }

    public static class HiveRowValue extends AbstractRowValue<HiveTables, HiveColumn, HiveConstant> {

        protected HiveRowValue(HiveTables tables, Map<HiveColumn, HiveConstant> values) {
            super(tables, values);
        }
    }

    public static class HiveTables extends AbstractTables<HiveTable, HiveColumn> {

        public HiveTables(List<HiveTable> tables) {
            super(tables);
        }
    }

    public static class HiveTable extends AbstractRelationalTable<HiveColumn, TableIndex, HiveGlobalState> {

        public HiveTable(String name, List<HiveColumn> columns, List<TableIndex> indexes, boolean isView) {
            super(name, columns, indexes, isView);
        }
    }

    public HiveSchema(List<HiveTable> databaseTables) {
        super(databaseTables);
    }

    public static HiveSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        // TODO
        List<HiveTable> databaseTables = new ArrayList<>();

    }
}
