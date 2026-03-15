package ydb.jimmer.dialect;

import org.babyfish.jimmer.sql.JSqlClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.jdbc.datasource.init.ScriptException;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import tech.ydb.test.junit5.YdbHelperExtension;
import ydb.jimmer.dialect.sqlMonitor.ExecutorMonitor;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    protected static final ExecutorMonitor executor = new ExecutorMonitor();
    private static final JSqlClient yqlClient;

    static {
        yqlClient = YqlClientBuilder.getBuilder()
                .setExecutor(executor)
                .build();
    }

    protected JSqlClient getYqlClient() {
        return yqlClient;
    }

    protected void initDatabase() {
        try (Connection connection = DriverManager.getConnection(getJdbcURL())) {
            URL dropTablesUrl = AbstractTest.class.getClassLoader().getResource("database-drop-tables-ydb.sql");
            if (dropTablesUrl == null) {
                throw new IllegalStateException("Cannot load 'database-drop-tables-ydb.sql'");
            }
            try {
                executeYqlScript(connection, dropTablesUrl);
            } catch (ScriptException e) {
                //
            }

            URL url = AbstractTest.class.getClassLoader().getResource("database-ydb.sql");
            if (url == null) {
                throw new IllegalStateException("Cannot load 'database-ydb.sql'");
            }
            executeYqlScript(connection, url);
        } catch (SQLException e) {
            Assertions.fail("Database threw an exception: " + e.getMessage());
        }
    }

    private void executeYqlScript(Connection connection, URL url) throws ScriptException {
        ScriptUtils.executeSqlScript(
                connection,
                new EncodedResource(new UrlResource(url)),
                false,
                false,
                "--",
                ";",
                "/*",
                "*/");
    }

    protected String getJdbcURL() {
        StringBuilder jdbc = new StringBuilder("jdbc:ydb:")
                .append(ydb.useTls() ? "grpcs://" : "grpc://")
                .append(ydb.endpoint())
                .append("/")
                .append(ydb.database());

        if (ydb.authToken() != null) {
            jdbc.append("?").append("token=").append(ydb.authToken());
        }

        return jdbc.toString();
    }

    protected void createTable(String tableName, String typeName) {
        executeSql(
                "CREATE TABLE " + tableName + "(" +
                        "id Int8," +
                        "value " + typeName + "," +
                        "PRIMARY KEY (id)" +
                        ")");
    }

    protected void executeSql(String sql) {
        try (Connection connection = DriverManager.getConnection(getJdbcURL())) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            Assertions.fail("Database threw an exception: " + e.getMessage());
        }
    }
}
