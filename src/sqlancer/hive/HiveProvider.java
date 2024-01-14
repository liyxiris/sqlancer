package sqlancer.hive;

import sqlancer.AbstractAction;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveProvider extends SQLProviderAdapter<HiveGlobalState, HiveOptions> {

    protected HiveProvider() {
        super(HiveGlobalState.class, HiveOptions.class);
    }

    public enum Action implements AbstractAction<HiveGlobalState> {

        INSERT()
    }

    @Override
    public void generateDatabase(HiveGlobalState globalState) throws Exception {
        // TODO
    }

    @Override
    public SQLConnection createDatabase(HiveGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        String databaseName = globalState.getDatabaseName();

        String url = String.format("jdbc:hive2://%s:%d/%s", host, port, databaseName);
        Connection con = DriverManager.getConnection(url, username, password);

        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);

        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }

        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "hive";
    }
}
