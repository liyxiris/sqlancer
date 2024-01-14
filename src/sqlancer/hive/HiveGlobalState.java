package sqlancer.hive;

import sqlancer.SQLGlobalState;
import org.apache.hive.jdbc.HiveDriver;

public class HiveGlobalState extends SQLGlobalState<HiveOptions, HiveSchema> {

    @Override
    protected HiveSchema readSchema() throws Exception {
        return HiveSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
