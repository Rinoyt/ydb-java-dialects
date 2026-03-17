package ydb.jimmer.dialect.sqlMonitor;

import org.babyfish.jimmer.sql.runtime.ExceptionTranslator;
import org.babyfish.jimmer.sql.runtime.ExecutionPurpose;
import org.babyfish.jimmer.sql.runtime.Executor;
import org.babyfish.jimmer.sql.runtime.ExecutorContext;
import org.babyfish.jimmer.sql.runtime.JSqlClientImplementor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class BatchExecutorMonitor implements Executor.BatchContext {
    private final List<QueryLog> queryLogs;
    private final Executor.BatchContext raw;
    private final List<List<Object>> variablesList = new ArrayList<>();

    public BatchExecutorMonitor(List<QueryLog> queryLogs, Executor.BatchContext raw) {
        this.queryLogs  = queryLogs;
        this.raw = raw;
    }

    @Override
    public JSqlClientImplementor sqlClient() {
        return raw.sqlClient();
    }

    @Override
    public String sql() {
        return raw.sql();
    }

    @Override
    public ExecutionPurpose purpose() {
        return raw.purpose();
    }

    @Override
    public ExecutorContext ctx() {
        return raw.ctx();
    }

    @Override
    public void add(List<Object> variables) {
        raw.add(variables);
        variablesList.add(variables);
    }

    @Override
    public int[] execute(BiFunction<SQLException, ExceptionTranslator.Args, Exception> exceptionTranslator) {
        queryLogs.add(new QueryLog(raw.sql(), raw.purpose(), variablesList));
        return raw.execute(exceptionTranslator);
    }

    @Override
    public Object[] generatedIds() {
        return raw.generatedIds();
    }

    @Override
    public void addExecutedListener(Runnable listener) {
        raw.addExecutedListener(listener);
    }

    @Override
    public void close() {
        raw.close();
    }
}
