package ydb.jimmer.dialect.constant;

public final class YdbConst {
    private YdbConst() {}

    public static final int SQL_KIND_PRIMITIVE = 10000;
    public static final int SQL_KIND_DECIMAL = 1 << 14; // 16384

    public static final int ONLINE_CONSISTENT_READ_ONLY = 16;
    public static final int ONLINE_INCONSISTENT_READ_ONLY = ONLINE_CONSISTENT_READ_ONLY + 1;
    public static final int STALE_READ_ONLY = 32;
}
