package ydb.jimmer.dialect.scalar;

import org.babyfish.jimmer.sql.runtime.AbstractScalarProvider;
import org.babyfish.jimmer.sql.runtime.Reader;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class DumbYdbScalarProvider<T> extends AbstractScalarProvider<T, T> {
    private final Class<T> clazz;

    public DumbYdbScalarProvider(Class<T> clazz) {
        super(clazz, clazz);

        this.clazz =  clazz;
    }

    @Override
    public T toScalar(@NotNull T sqlValue) {
        return sqlValue;
    }

    @Override
    public T toSql(@NotNull T scalarValue) {
        return scalarValue;
    }

    @Override
    public Reader<T> reader() {
        return new DumbReader();
    }

    private class DumbReader implements Reader<T> {
        @Override
        public T read(ResultSet rs, Context ctx) throws SQLException {
            return rs.getObject(ctx.col(), clazz);
        }

        @Override
        public void skip(Context ctx) {
            ctx.col();
        }
    }
}
