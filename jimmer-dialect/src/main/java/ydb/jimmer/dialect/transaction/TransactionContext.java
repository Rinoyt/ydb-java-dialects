package ydb.jimmer.dialect.transaction;

public class TransactionContext {
    private static final ThreadLocal<TransactionSettings> LOCAL_CONTEXT = new ThreadLocal<>();

    public static void setSettings(int isolationLevel) {
        LOCAL_CONTEXT.set(new TransactionSettings(isolationLevel));
    }

    public static TransactionSettings getSettings() {
        return LOCAL_CONTEXT.get();
    }

    public static void clear() {
        LOCAL_CONTEXT.remove();
    }

    public static class TransactionSettings {
        final int isolationLevel;

        TransactionSettings(int isolationLevel) {
            this.isolationLevel = isolationLevel;
        }
    }
}
