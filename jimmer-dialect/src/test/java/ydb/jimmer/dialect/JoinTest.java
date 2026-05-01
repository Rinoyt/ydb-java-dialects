package ydb.jimmer.dialect;

import org.babyfish.jimmer.sql.JoinType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ydb.jimmer.dialect.model.StudentTable;

public class JoinTest extends AbstractSelectTest {
    @BeforeEach
    protected void setup() {
        initDatabase();
    }

    @Test
    public void leftJoinTest() {
        StudentTable table = StudentTable.$;
        executeAndExpect(
                getYqlClient()
                        .createQuery(table)
                        .orderBy(table.group(JoinType.LEFT).name().asc())
                        .select(table),
                cxt -> cxt.sql(
                        "select tb_1_.id, tb_1_.name, tb_1_.group " +
                                "from student tb_1_ " +
                                "left join group tb_2_ on tb_1_.group = tb_2_.id " +
                                "order by tb_2_.name asc"
                )
        );
    }
}
