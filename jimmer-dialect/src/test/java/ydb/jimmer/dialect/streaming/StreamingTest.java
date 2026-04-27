package ydb.jimmer.dialect.streaming;

import org.babyfish.jimmer.sql.ast.PropExpression;
import org.babyfish.jimmer.sql.ast.table.spi.AbstractTypedTable;
import org.junit.jupiter.api.Test;
import ydb.jimmer.dialect.AbstractSelectTest;
import ydb.jimmer.dialect.model.streaming.YdbStreamingTable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class StreamingTest extends AbstractSelectTest {
    @Test
    public void cursorTest() {
        String tableName =  "ydb_streaming";
        String typeName =  "Int32";
        AbstractTypedTable<?> table = YdbStreamingTable.$;
        PropExpression<?> prop = YdbStreamingTable.$.value();
        String[] valuesToInsert = new String[]{"11", "12", "21", "22"};
        String[] expectedValues = new String[]{"11", "12", "21", "22"};

        createTable(tableName, typeName);

        insert(tableName, valuesToInsert);

        String json = buildJsonResponse(expectedValues);

        executeAndExpect((Connection con) -> {
                    List<Object> responses = new ArrayList<>();
                    getYqlClient()
                            .createQuery(table)
                            .orderBy(prop)
                            .select(table)
                            .forEach(con, 2, responses::add);
                    return responses;
                },
                cxt -> {
                    cxt.sql(
                            "select tb_1_.id, tb_1_.value from " + tableName + " tb_1_ order by tb_1_.value asc");
                    cxt.rows(json);
                }
        );
    }
}
