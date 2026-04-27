package ydb.jimmer.dialect;

import org.babyfish.jimmer.sql.ast.Executable;
import org.junit.jupiter.api.Assertions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public abstract class AbstractSelectTest extends AbstractTest {
    protected static <R> void executeAndExpect(Executable<? extends List<R>> query, Consumer<QueryTestContext> block) {
        List<R> rows = connectAndExecute(true, query);
        block.accept(new QueryTestContext(executor.getLogs(), rows));
    }

    private static <R> List<R> connectAndExecute(boolean rollback, Executable<? extends List<R>> query) {
        try (Connection connection = DriverManager.getConnection(getJdbcURL())) {
            connection.setAutoCommit(!rollback);
            try {
                return query.execute(connection);
            } finally {
                if (rollback) {
                    connection.rollback();
                }
            }
        } catch (SQLException e) {
            Assertions.fail("Database threw an exception: " + e.getMessage());
        }

        return null;
    }

    protected static void insert(String tableName, String... values) {
        if (values.length == 0) {
            return;
        }
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (id, value) VALUES ");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("(").append(i).append(", ").append(values[i]).append(")");
        }
        executeSql(sql.toString());
    }

    protected static String buildJsonResponse(String[] expectedValues) {
        return buildJsonResponse(0, expectedValues);
    }

    protected static String buildJsonResponse(int startingId, String[] expectedValues) {
        StringBuilder json = new StringBuilder("[");
        for (int i  = 0; i < expectedValues.length; i++) {
            if (json.length() != 1) {
                json.append(",");
            }
            json.append("{");
            json.append("\"id\":").append(startingId + i).append(",\"value\":").append(expectedValues[i]);
            json.append("}");
        }
        json.append("]");

        return json.toString();
    }
}
