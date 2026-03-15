package ydb.jimmer.dialect;

import org.babyfish.jimmer.sql.ast.query.TypedRootQuery;
import org.junit.jupiter.api.Assertions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public abstract class AbstractSelectTest extends AbstractTest {
    protected <R> void executeAndExpect(TypedRootQuery<R> query, Consumer<QueryTestContext> block) {
        List<R> rows = connectAndExecute(true, query);
        block.accept(new QueryTestContext(executor.getLogs(), rows));
    }

    private <R> List<R> connectAndExecute(boolean rollback, TypedRootQuery<R> query) {
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
}
