package ydb.jimmer.dialect.batch;

import org.babyfish.jimmer.sql.TargetTransferMode;
import org.babyfish.jimmer.sql.ast.mutation.SaveMode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ydb.jimmer.dialect.AbstractInsertTest;
import ydb.jimmer.dialect.model.type.ydbInt32.YdbIntDraft;

import java.util.Arrays;

public class BatchTest  extends AbstractInsertTest {
    private static final String TABLE_NAME = "ydb_int";

    private void batchTest(SaveMode saveMode, String sql) {
        executeAndExpect(
                getYqlClientForBatch().getEntities().saveEntitiesCommand(
                        Arrays.asList(
                            YdbIntDraft.$.produce(t -> {
                                t.setId(0);
                                t.setValue(123);
                            }),
                            YdbIntDraft.$.produce(t -> {
                                t.setId(1);
                                t.setValue(456);
                            })
                        ))
                        .setMode(saveMode),
                cxt -> {
                    cxt.sql(sql);
                    cxt.batchVariables(0, 0, 123);
                    cxt.batchVariables(1, 1, 456);
                }
        );
    }

    @BeforeAll
    static void setup() {
        createTable(TABLE_NAME, "Int32");
    }

    @Test
    public void insertTest() {
        batchTest(SaveMode.INSERT_ONLY,
                "insert into " + TABLE_NAME + "(id, value) values(?, ?)");
    }

    @Test
    public void updateTest() {
        batchTest(SaveMode.UPDATE_ONLY,
                "update " + TABLE_NAME + " set value = ? where id = ? returning id");
    }

    @Test
    public void upsertTest() {
        batchTest(SaveMode.UPSERT,
                "upsert into " + TABLE_NAME + "(id, value) values(?, ?)");
    }
}
