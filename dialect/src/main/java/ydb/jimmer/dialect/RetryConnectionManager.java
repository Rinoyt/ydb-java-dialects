package ydb.jimmer.dialect;

import org.babyfish.jimmer.sql.transaction.Propagation;
import org.babyfish.jimmer.sql.transaction.TxConnectionManager;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.function.Function;

/**
 * Provides retry from abort/timeout for transactions.
 * While using frameworks like spring this class is redundant.
 * With spring the "@Retryable" annotation is preferable.
 */
public class RetryConnectionManager implements TxConnectionManager {
    private final DataSource dataSource;
    private final int maxRetries;
    private final long retryDelayMs;

    public RetryConnectionManager(DataSource dataSource, int maxRetries, long retryDelayMs) {
        this.dataSource = dataSource;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    @Override
    public <R> R executeTransaction(Propagation propagation, Function<Connection, R> block) {
        for (int i = 0; i < maxRetries; i++) {
            try (Connection connection = dataSource.getConnection()) {
                try {
                    connection.setAutoCommit(false);
                    R result = block.apply(connection);
                    connection.commit();
                    return result;
                } catch (SQLException ex) {
                    connection.rollback();
                    if (i == maxRetries - 1 || !isRetryable(ex)) {
                        throw new RuntimeException(ex);
                    }
                }
            } catch (SQLException ex) {
                if (i == maxRetries - 1 || !isRetryable(ex)) {
                    throw new RuntimeException(ex);
                }
            }
            try {
                Thread.sleep(retryDelayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
        }

        throw new RuntimeException("Max retries exceeded");
    }

    @Override
    public <R> R execute(@Nullable Connection con, Function<Connection, R> block) {
        if (con != null) {
            // No connection management, no transaction management, everything is controlled by user.
            return block.apply(con);
        }

        for (int i = 0; i < maxRetries; i++) {
            try (Connection connection = dataSource.getConnection()) {
                return block.apply(connection);
            } catch (SQLException ex) {
                if (i == maxRetries - 1 || !isRetryable(ex)) {
                    throw new RuntimeException(ex);
                }
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }

        throw new RuntimeException("Max retries exceeded");
    }

    private boolean isRetryable(Exception ex) {
        return ex instanceof SQLTimeoutException ||
                (ex instanceof SQLException && isRetryableSqlState((SQLException) ex));
    }

    private boolean isRetryableSqlState(SQLException ex) {
        String sqlState = ex.getSQLState();
        return sqlState != null && (
                sqlState.startsWith("40") || // Transaction rollback
                        "08S01".equals(sqlState)   // Communication link failure
        );
    }
}
