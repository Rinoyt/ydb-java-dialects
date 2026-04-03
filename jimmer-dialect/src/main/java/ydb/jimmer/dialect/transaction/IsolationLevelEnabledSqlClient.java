package ydb.jimmer.dialect.transaction;

import org.babyfish.jimmer.sql.di.AbstractJSqlClientDelegate;
import org.babyfish.jimmer.sql.runtime.JSqlClientImplementor;

import java.sql.Connection;
import java.util.function.Supplier;

public class IsolationLevelEnabledSqlClient extends AbstractJSqlClientDelegate {
    private final JSqlClientImplementor delegate;

    public IsolationLevelEnabledSqlClient(JSqlClientImplementor delegate) {
        this.delegate = delegate;
    }

    @Override
    protected JSqlClientImplementor sqlClient() {
        return delegate;
    }

    public <R> R withIsolation(int isolationLevel, Supplier<R> block) {
        try {
            TransactionContext.setSettings(isolationLevel);
            return transaction(block);
        } finally {
            TransactionContext.clear();
        }
    }

    public <R> R withSnapshotIsolation(Supplier<R> block) {
        return withIsolation(Connection.TRANSACTION_REPEATABLE_READ, block);
    }

    public <R> R withReadCommitted(Supplier<R> block) {
        return withIsolation(Connection.TRANSACTION_READ_COMMITTED, block);
    }
}
