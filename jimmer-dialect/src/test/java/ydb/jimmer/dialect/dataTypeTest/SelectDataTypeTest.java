package ydb.jimmer.dialect.dataTypeTest;

import org.babyfish.jimmer.sql.ast.PropExpression;
import org.babyfish.jimmer.sql.ast.table.spi.AbstractTypedTable;
import org.junit.jupiter.api.Test;
import ydb.jimmer.dialect.AbstractSelectTest;
import ydb.jimmer.dialect.model.type.ydbBool.YdbBooleanClassTable;
import ydb.jimmer.dialect.model.type.ydbBool.YdbBooleanTable;
import ydb.jimmer.dialect.model.type.ydbDate32.YdbDateTable;
import ydb.jimmer.dialect.model.type.ydbDate32.YdbLocalDateTable;
import ydb.jimmer.dialect.model.type.ydbDatetime64.YdbLocalDateTimeTable;
import ydb.jimmer.dialect.model.type.ydbDecimal.YdbBigDecimalTable;
import ydb.jimmer.dialect.model.type.ydbDouble.YdbDoubleClassTable;
import ydb.jimmer.dialect.model.type.ydbDouble.YdbDoubleTable;
import ydb.jimmer.dialect.model.type.ydbEnum.YdbEnumTable;
import ydb.jimmer.dialect.model.type.ydbFloat.YdbFloatClassTable;
import ydb.jimmer.dialect.model.type.ydbFloat.YdbFloatTable;
import ydb.jimmer.dialect.model.type.ydbInt16.YdbShortClassTable;
import ydb.jimmer.dialect.model.type.ydbInt16.YdbShortTable;
import ydb.jimmer.dialect.model.type.ydbInt32.YdbIntTable;
import ydb.jimmer.dialect.model.type.ydbInt32.YdbIntegerTable;
import ydb.jimmer.dialect.model.type.ydbInt32.YdbLocalTimeTable;
import ydb.jimmer.dialect.model.type.ydbInt32.YdbTimeTable;
import ydb.jimmer.dialect.model.type.ydbInt64.YdbBigIntegerTable;
import ydb.jimmer.dialect.model.type.ydbInt64.YdbLongClassTable;
import ydb.jimmer.dialect.model.type.ydbInt64.YdbLongTable;
import ydb.jimmer.dialect.model.type.ydbInt8.YdbByteClassTable;
import ydb.jimmer.dialect.model.type.ydbInt8.YdbByteTable;
import ydb.jimmer.dialect.model.type.ydbInterval64.YdbDurationTable;
import ydb.jimmer.dialect.model.type.ydbJson.YdbJsonTable;
import ydb.jimmer.dialect.model.type.ydbString.YdbByteArrayTable;
import ydb.jimmer.dialect.model.type.ydbTimestamp64.YdbInstantTable;
import ydb.jimmer.dialect.model.type.ydbTimestamp64.YdbTimestampTable;
import ydb.jimmer.dialect.model.type.ydbTimestamp64.YdbUtilDateTable;
import ydb.jimmer.dialect.model.type.ydbUtf8.YdbStringTable;
import ydb.jimmer.dialect.model.type.ydbUuid.YdbUuidTable;

public class SelectDataTypeTest extends AbstractSelectTest {
    private void insert(String tableName, String... values) {
        if (values.length == 0) {
            return;
        }
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (id, value) VALUES ");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("(").append(i).append(", ").append(values[i]).append(")");
        }
        executeSql(sql.toString());
    }

    private void typeTest(String tableName,
                          String typeName,
                          AbstractTypedTable<?> table,
                          PropExpression<?> prop,
                          String[] valuesToInsert,
                          String[] expectedValues) {
        createTable(tableName, typeName);

        insert(tableName, valuesToInsert);

        StringBuilder json = new StringBuilder("[");
        for (int i  = 0; i < valuesToInsert.length; i++) {
            if (json.length() != 1) {
                json.append(",");
            }
            json.append("{");
            json.append("\"id\":").append(i).append(",\"value\":").append(expectedValues[i]);
            json.append("}");
        }
        json.append("]");

        if (expectedValues.length == 1) {
            executeAndExpect(
                    getYqlClient()
                            .createQuery(table)
                            .select(table),
                    cxt -> {
                        cxt.sql(
                                "select tb_1_.id, tb_1_.value from " + tableName + " tb_1_");
                        cxt.rows(json.toString());
                    }
            );
        } else {
            executeAndExpect(
                    getYqlClient()
                            .createQuery(table)
                            .orderBy(prop)
                            .select(table),
                    cxt -> {
                        cxt.sql(
                                "select tb_1_.id, tb_1_.value from " + tableName + " tb_1_ order by tb_1_.value asc");
                        cxt.rows(json.toString());
                    }
            );
        }
    }

    @Test
    public void boolTest() {
        String[] valuesToInsert = new String[]{"false", "true"};
        String[] expectedValues = new String[]{"false", "true"};

        typeTest("ydb_boolean", "Bool",
                YdbBooleanTable.$, YdbBooleanTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_boolean_class", "Bool",
                YdbBooleanClassTable.$, YdbBooleanClassTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void date32Test() {
        String[] valuesToInsert = new String[]{"Date32(\"144169-01-01\")"};
        String[] expectedValues = new String[]{"\"4169-01-01\""};

        typeTest("ydb_date", "Date32",
                YdbDateTable.$, YdbDateTable.$.value(),
                valuesToInsert, expectedValues);

        expectedValues = new String[]{"\"+144169-01-01\""};

        typeTest("ydb_local_date", "Date32",
                YdbLocalDateTable.$, YdbLocalDateTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void dateTime64Test() {
        String[] valuesToInsert = new String[]{"DateTime64(\"2017-11-27T13:24:00Z\")"};
        String[] expectedValues = new String[]{"\"2017-11-27T13:24:00\""};

        typeTest("ydb_local_date_time", "DateTime64",
                YdbLocalDateTimeTable.$, YdbLocalDateTimeTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void decimalTest() {
        String[] valuesToInsert = new String[]{"Decimal(\"1.23\", 22, 9)"};
        String[] expectedValues = new String[]{"1.230000000"};

        typeTest("ydb_big_decimal", "Decimal(22, 9)",
                YdbBigDecimalTable.$, YdbBigDecimalTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void doubleTest() {
        String[] valuesToInsert = new String[]{"Double(\"1.23\")"};
        String[] expectedValues = new String[]{"1.23"};

        typeTest("ydb_double", "Double",
                YdbDoubleTable.$, YdbDoubleTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_double_class", "Double",
                YdbDoubleClassTable.$, YdbDoubleClassTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void enumTest() {
        String[] valuesToInsert = new String[]{"\"ONE\"", "\"TWO\""};
        String[] expectedValues = new String[]{"\"ONE\"", "\"TWO\""};

        typeTest("ydb_enum", "Utf8",
                YdbEnumTable.$, YdbEnumTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void floatTest() {
        String[] valuesToInsert = new String[]{"Float(\"1.23\")"};
        String[] expectedValues = new String[]{"1.23"};

        typeTest("ydb_float", "Float",
                YdbFloatTable.$, YdbFloatTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_float_class", "Float",
                YdbFloatClassTable.$, YdbFloatClassTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void int8Test() {
        String[] valuesToInsert = new String[]{"-1", "0", "10"};
        String[] expectedValues = new String[]{"-1", "0", "10"};

        typeTest("ydb_byte", "Int8",
                YdbByteTable.$, YdbByteTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_byte_class", "Int8",
                YdbByteClassTable.$, YdbByteClassTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void int16Test() {
        String[] valuesToInsert = new String[]{"-1", "0", "10"};
        String[] expectedValues = new String[]{"-1", "0", "10"};

        typeTest("ydb_short", "Int16",
                YdbShortTable.$, YdbShortTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_short_class", "Int16",
                YdbShortClassTable.$, YdbShortClassTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void int32Test() {
        String[] valuesToInsert = new String[]{"-1", "0", "10"};
        String[] expectedValues = new String[]{"-1", "0", "10"};

        typeTest("ydb_int", "Int32",
                YdbIntTable.$, YdbIntTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_integer", "Int32",
                YdbIntegerTable.$, YdbIntegerTable.$.value(),
                valuesToInsert, expectedValues);

        expectedValues = new String[]{"\"02:59:59.999\"", "\"03:00:00\"", "\"03:00:00.01\""};

        typeTest("ydb_local_time", "Int32",
                YdbLocalTimeTable.$, YdbLocalTimeTable.$.value(),
                valuesToInsert, expectedValues);

        valuesToInsert = new String[]{"0", "10"};
        expectedValues = new String[]{"\"00:00:00\"", "\"00:00:10\""};

        typeTest("ydb_time", "Int32",
                YdbTimeTable.$, YdbTimeTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void int64Test() {
        String[] valuesToInsert = new String[]{"-1", "0", "10"};
        String[] expectedValues = new String[]{"-1", "0", "10"};

        typeTest("ydb_big_integer", "Int64",
                YdbBigIntegerTable.$, YdbBigIntegerTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_long", "Int64",
                YdbLongTable.$, YdbLongTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_long_class", "Int64",
                YdbLongClassTable.$, YdbLongClassTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void interval64Test() {
        String[] valuesToInsert = new String[]{"Interval(\"P0DT0H0M0.567890S\")"};
        String[] expectedValues = new String[]{"0.567890000"};

        typeTest("ydb_duration", "Interval64",
                YdbDurationTable.$, YdbDurationTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void jsonTest() {
        String[] valuesToInsert = new String[]{"Json(@@{\"a\":1,\"b\":null}@@)"};
        String[] expectedValues = new String[]{"{\"a\":1,\"b\":null}"};

        typeTest("ydb_json", "Json",
                YdbJsonTable.$, YdbJsonTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void stringTest() {
        String[] valuesToInsert = new String[]{"\"0\"", "\"string\""};
        String[] expectedValues = new String[]{"\"MA==\"", "\"c3RyaW5n\""};

        typeTest("ydb_byte_array", "String",
                YdbByteArrayTable.$, YdbByteArrayTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void timestamp64Test() {
        String[] valuesToInsert = new String[]{"Timestamp64(\"2017-11-27T13:24:00.123456Z\")"};
        String[] expectedValues = new String[]{"\"2017-11-27T13:24:00.123456Z\""};

        typeTest("ydb_instant", "Timestamp64",
                YdbInstantTable.$, YdbInstantTable.$.value(),
                valuesToInsert, expectedValues);

        expectedValues = new String[]{"\"2017-11-27\""};

        typeTest("ydb_timestamp", "Timestamp64",
                YdbTimestampTable.$, YdbTimestampTable.$.value(),
                valuesToInsert, expectedValues);

        typeTest("ydb_util_date", "Timestamp64",
                YdbUtilDateTable.$, YdbUtilDateTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void utf8Test() {
        String[] valuesToInsert = new String[]{"\"0\"", "\"string\""};
        String[] expectedValues = new String[]{"\"0\"", "\"string\""};

        typeTest("ydb_string", "Utf8",
                YdbStringTable.$, YdbStringTable.$.value(),
                valuesToInsert, expectedValues);
    }

    @Test
    public void uuidTest() {
        String[] valuesToInsert = new String[]{
                "Uuid(\"9e197d65-1914-4d57-a65f-77a52a06baa7\")",
                "Uuid(\"8e0f2cf4-4656-4d73-970e-a18be9ead78b\")"};
        String[] expectedValues = new String[]{
                "\"9e197d65-1914-4d57-a65f-77a52a06baa7\"",
                "\"8e0f2cf4-4656-4d73-970e-a18be9ead78b\""};

        typeTest("ydb_uuid", "Uuid",
                YdbUuidTable.$, YdbUuidTable.$.value(),
                valuesToInsert, expectedValues);
    }
}
