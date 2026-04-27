package ydb.jimmer.dialect.pagination;

import org.junit.jupiter.api.Test;
import ydb.jimmer.dialect.AbstractSelectTest;
import ydb.jimmer.dialect.QueryTestContext;
import ydb.jimmer.dialect.YdbKeysetPaginator;
import ydb.jimmer.dialect.model.Table;
import ydb.jimmer.dialect.model.YdbIntTable;

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

    @Test
    public void simpleTest() {
        createTable(TABLE_NAME, VALUE_TYPE_NAME);
        insert(TABLE_NAME, VALUES);

        YdbKeysetPaginator paginator = new YdbKeysetPaginator(getIsolationClient());

        YdbIntTable table = YdbIntTable.$;

        YdbKeysetPaginator.Page<Table> page = null;
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
                    item -> List.of(((Table) item).getId()),
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
