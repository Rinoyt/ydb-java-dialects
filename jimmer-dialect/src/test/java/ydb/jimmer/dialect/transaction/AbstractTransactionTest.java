package ydb.jimmer.dialect.transaction;

import org.babyfish.jimmer.sql.ast.mutation.MutationResult;
import org.babyfish.jimmer.sql.ast.mutation.SaveMode;
import ydb.jimmer.dialect.AbstractSelectTest;
import ydb.jimmer.dialect.QueryTestContext;
import ydb.jimmer.dialect.model.transaction.YdbTransaction;
import ydb.jimmer.dialect.model.transaction.YdbTransactionDraft;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractTransactionTest extends AbstractSelectTest {
    protected static final IsolationEnabledSqlClient yqlClient = getIsolationClient();

    protected void readTest(Function<Supplier<List<YdbTransaction>>, List<YdbTransaction>> transaction) {
        String tableName = "ydb_transaction";
        String typeName = "Int32";
        String[] valuesToInsert = new String[]{"-1", "0", "10"};
        String[] expectedValues = new String[]{"-1", "0", "10"};

        createTable(tableName, typeName);

        insert(tableName, valuesToInsert);

        List<YdbTransaction> rows = transaction.apply(() ->
                yqlClient.getEntities().findAll(YdbTransaction.class)
        );
        QueryTestContext cxt = new QueryTestContext(executor.getLogs(), rows);

        cxt.sql("select tb_1_.id, tb_1_.value from " + tableName + " tb_1_");

        String json = buildJsonResponse(expectedValues);
        cxt.rows(json);

        dropTable(tableName);
    }

    protected void writeTest(Function<Supplier<MutationResult>, MutationResult> transaction, boolean readOnly) {
        String errorMessage = null;
        if (readOnly) {
            errorMessage = "Cannot execute the DML statement";
        }

        writeTest(transaction, errorMessage);
    }

    protected void writeTest(Function<Supplier<MutationResult>, MutationResult> transaction, String errorMessage) {
        String tableName = "ydb_transaction";
        String typeName = "Int32";
        Object[] variables = new Object[]{0, 10};

        createTable(tableName, typeName);

        MutationResult result = null;
        Throwable throwable = null;
        try {
            result = transaction.apply(() ->
                    yqlClient.getEntities().saveCommand(
                            YdbTransactionDraft.$.produce(item -> {
                                item.setId(0);
                                item.setValue(10);
                            })
                    ).setMode(SaveMode.INSERT_ONLY).execute()
            );
        } catch (Throwable ex) {
            throwable = ex;
        }
        QueryTestContext cxt = new QueryTestContext(executor.getLogs(), result, throwable);

        cxt.sql("insert into " + tableName + "(id, value) values(?, ?)");
        cxt.variables(variables);
        cxt.error(errorMessage);

        dropTable(tableName);
    }
}
