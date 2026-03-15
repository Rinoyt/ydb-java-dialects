package ydb.jimmer.dialect;

import org.babyfish.jimmer.sql.JSqlClient;
import ydb.jimmer.dialect.scalar.DurationProvider;
import ydb.jimmer.dialect.scalar.InstantProvider;

public final class YqlClientBuilder {
    private YqlClientBuilder() {}

    public static JSqlClient.Builder getBuilder() {
        return addScalarProviders(
                JSqlClient.newBuilder()
                .setDialect(new YdbDialect())
        );
    }

    public static JSqlClient getYqlClient() {
        return getBuilder().build();
    }

    public static JSqlClient.Builder addScalarProviders(JSqlClient.Builder builder) {
        return builder
                .addScalarProvider(new InstantProvider())
                .addScalarProvider(new DurationProvider());
    }
}
