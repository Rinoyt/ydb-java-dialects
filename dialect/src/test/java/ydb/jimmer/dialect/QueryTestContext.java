package ydb.jimmer.dialect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.babyfish.jimmer.jackson.ImmutableModule;
import org.babyfish.jimmer.sql.ast.mutation.MutationResult;
import org.babyfish.jimmer.sql.collection.TypedList;
import org.junit.jupiter.api.Assertions;
import ydb.jimmer.dialect.sqlMonitor.QueryLog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryTestContext {
    private static final ObjectMapper MAPPER = new ObjectMapper()
                    .registerModule(new ImmutableModule())
                    .registerModule(new JavaTimeModule());

    static {
        MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd"));
    }

    private final List<QueryLog> logs;
    private final List<?> rows;
    private final MutationResult result;
    private final Throwable throwable;

    private int index = 0;

    public QueryTestContext(List<QueryLog> logs, List<?> rows) {
        this.logs = logs;
        this.rows = rows;
        result = null;
        throwable = null;
    }

    public QueryTestContext(List<QueryLog> logs, MutationResult result, Throwable throwable) {
        this.logs = logs;
        this.rows = new ArrayList<>();
        this.result = result;
        this.throwable = throwable;
    }

    public void nextStatement() {
        index++;
    }

    public void sql(String sql) {
        Assertions.assertEquals(
                sql.replace("--->", "").toLowerCase(),
                logs.get(index).getSql().toLowerCase(),
                "statements[" + index + "].sql");
    }

    public void variables(Object[][] values) {
        Assertions.assertEquals(
                values.length,
                logs.get(index).getVariablesList().size(),
                "statements[" + index + "] actual batch size = " +
                        logs.get(index).getVariablesList().size() +
                        ", but expected batch size = " +
                        values.length
        );

        for (int i = 0; i < values.length; i++) {
            Assertions.assertEquals(
                    values[i].length,
                    logs.get(index).getVariablesList().get(i).size(),
                    "statements[" + index + "].batch[" + i + "] actual number of variables = " +
                            logs.get(index).getVariablesList().get(i).size() +
                            ", but expected number of variables = " +
                            values[i].length
            );
        }

        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values[i].length; j++) {
                Object expect = values[i][j];

                Object actual = logs.get(index).getVariablesList().get(i).get(j);

                Assertions.assertEquals(
                        expect,
                        actual,
                        "statements[" + index + "].batch[" + i + "].variables[" + j + "] actual value = " +
                                actual +
                                ", but expected value = " +
                                expect
                );
            }
        }
    }

    public void rows(String json) {
        try {
            Assertions.assertEquals(
                    json.replace("--->", ""),
                    MAPPER.writeValueAsString(rows)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
