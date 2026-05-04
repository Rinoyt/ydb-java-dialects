package ydb.jimmer.dialect.transaction;

import org.babyfish.jimmer.sql.transaction.AbstractTxConnectionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Sets transaction isolation level and read only mode
 * to the connection at the start of a transaction.
 */
public class YdbTxConnectionManager extends AbstractTxConnectionManager {
    private final DataSource dataSource;

    public YdbTxConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    protected Connection openConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    protected void startTransaction(Connection con) throws SQLException {
        super.startTransaction(con);

        TransactionContext.TransactionSettings settings = TransactionContext.getSettings();
        if (settings != null) {
            if (settings.isolationLevel != Connection.TRANSACTION_NONE) {
                con.setTransactionIsolation(settings.isolationLevel);
            }
            con.setReadOnly(settings.readOnly);
        }
    }
}
