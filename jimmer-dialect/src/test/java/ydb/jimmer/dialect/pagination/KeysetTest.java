package ydb.jimmer.dialect.pagination;

import org.babyfish.jimmer.sql.JSqlClient;
import org.babyfish.jimmer.sql.runtime.JSqlClientImplementor;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ydb.jimmer.dialect.AbstractSelectTest;
import ydb.jimmer.dialect.QueryTestContext;
import ydb.jimmer.dialect.YdbKeysetPaginator;
import ydb.jimmer.dialect.model.YdbInt;
import ydb.jimmer.dialect.model.YdbIntTable;
import ydb.jimmer.dialect.transaction.IsolationEnabledSqlClient;
import ydb.jimmer.dialect.transaction.YdbTxConnectionManager;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;

public class KeysetTest extends AbstractSelectTest {
    private static final String TABLE_NAME = "simple_table";
    private static final String VALUE_TYPE_NAME = "Int32";

    private static final int N = 100;
    private static final int LIMIT = 20;
    private static final String[] VALUES = new String[100];

    static {
        for (int i = 0; i < N; i++) {
            VALUES[i] = String.valueOf(i);
        }
    }

    private static final DataSource dataSource = new DriverManagerDataSource(getJdbcURL());
    protected static final IsolationEnabledSqlClient yqlClient = new IsolationEnabledSqlClient(
            (JSqlClientImplementor) JSqlClient.newBuilder()
                    .setConnectionManager(new YdbTxConnectionManager(dataSource))
                    .setExecutor(executor)
                    .build()
    );

    @Test
    public void simpleTest() {
        createTable(TABLE_NAME, VALUE_TYPE_NAME);
        insert(TABLE_NAME, VALUES);

        YdbKeysetPaginator paginator = new YdbKeysetPaginator(yqlClient);

        YdbIntTable table = YdbIntTable.$;

        YdbKeysetPaginator.Page<YdbInt> page = null;
        for (int i = 0; i < N; i += LIMIT) {
            List<Object> nextCursor = null;
            if (page != null) {
                nextCursor = page.getNextCursor();
            }

            page = paginator.fetchPage(
                    table,
                    List.of(table.id()),
                    nextCursor,
                    LIMIT,
                    item -> List.of(((YdbInt) item).getId()),
                    (q, t) -> {
                        q.orderBy(t.id().asc());
                        return q.select(t);
                    }
            );

            QueryTestContext cxt = new QueryTestContext(executor.getLogs(), page.getRows());

            StringBuilder expectedSql = new StringBuilder("select tb_1_.id, tb_1_.value from " + TABLE_NAME + " tb_1_");
            if (i != 0) {
                expectedSql.append(" where (tb_1_.id) > (?)");
            }
            expectedSql.append(" order by tb_1_.id asc limit ?");

            cxt.sql(expectedSql.toString());

            String json = buildJsonResponse(i, Arrays.copyOfRange(VALUES, i, i + LIMIT));
            cxt.rows(json);
        }

        dropTable(TABLE_NAME);
    }
}
