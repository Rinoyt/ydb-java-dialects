package ydb.jimmer.dialect.model;

import org.babyfish.jimmer.sql.Column;
import org.babyfish.jimmer.sql.Entity;
import org.babyfish.jimmer.sql.Id;
import org.babyfish.jimmer.sql.Table;

@Entity
@Table(name = "simple_table")
public interface YdbInt {
    @Id
    @Column(name = "id")
    int getId();

    @Column(name = "value")
    int value();
}
