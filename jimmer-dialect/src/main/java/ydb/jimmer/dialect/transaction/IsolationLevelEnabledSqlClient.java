package ydb.jimmer.dialect.transaction;

import org.babyfish.jimmer.sql.di.AbstractJSqlClientDelegate;
import org.babyfish.jimmer.sql.runtime.JSqlClientImplementor;
import ydb.jimmer.dialect.constant.YdbConst;

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

    public <R> R serializableReadWrite(Supplier<R> block) {
        return withIsolation(Connection.TRANSACTION_SERIALIZABLE, block);
    }

    public <R> R snapshotReadOnly(Supplier<R> block) {
        return withIsolation(Connection.TRANSACTION_SERIALIZABLE, block);
    }

    public <R> R staleReadOnly(Supplier<R> block) {
        return withIsolation(YdbConst.STALE_READ_ONLY, block);
    }

    public <R> R onlineConsistentReadOnly(Supplier<R> block) {
        return withIsolation(YdbConst.ONLINE_CONSISTENT_READ_ONLY, block);
    }

    public <R> R onlineInconsistentReadOnly(Supplier<R> block) {
        return withIsolation(YdbConst.ONLINE_INCONSISTENT_READ_ONLY, block);
    }
}
