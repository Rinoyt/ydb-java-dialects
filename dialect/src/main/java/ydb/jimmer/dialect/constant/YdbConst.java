package ydb.jimmer.dialect.constant;

public final class YdbConst {
    private YdbConst() {}

    public static final int SQL_KIND_PRIMITIVE = 10000;
    public static final int SQL_KIND_DECIMAL = 1 << 14; // 16384
}
