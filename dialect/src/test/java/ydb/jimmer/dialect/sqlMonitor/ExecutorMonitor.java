package ydb.jimmer.dialect.sqlMonitor;

import org.babyfish.jimmer.meta.ImmutableProp;
import org.babyfish.jimmer.sql.runtime.DefaultExecutor;
import org.babyfish.jimmer.sql.runtime.ExecutionPurpose;
import org.babyfish.jimmer.sql.runtime.Executor;
import org.babyfish.jimmer.sql.runtime.JSqlClientImplementor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ExecutorMonitor implements Executor {
    private final Executor executor = DefaultExecutor.INSTANCE;

    private List<QueryLog> queryLogs = new ArrayList<>();

    @Override
    public <R> R execute(@NotNull Args<R> args) {
        queryLogs.add(QueryLog.simple(args.sql, args.purpose, args.variables));
        return executor.execute(args);
    }

    @Override
    public BatchContext executeBatch(
            @NotNull Connection con,
            @NotNull String sql,
            @Nullable ImmutableProp generatedIdProp,
            @NotNull ExecutionPurpose purpose,
            @NotNull JSqlClientImplementor sqlClient
    ) {
        return executor.executeBatch(con, sql, generatedIdProp, purpose, sqlClient);
    }

    public List<QueryLog> getLogs() {
        List<QueryLog> tmp = queryLogs;
        queryLogs = new ArrayList<>();
        return tmp;
    }
}
