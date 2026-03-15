package ydb.jimmer.dialect.scalar;

import java.time.Instant;

public class InstantProvider extends DumbYdbScalarProvider<Instant> {
    public InstantProvider() {
        super(Instant.class);
    }
}
