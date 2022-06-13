package io.delta.issue362;

import java.util.Arrays;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class TestRow {

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("id", new IntType()),
        new RowType.RowField("c01", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c02", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c03", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c04", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c05", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c06", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c07", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c08", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c09", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c10", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c11", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c12", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c13", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c14", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c15", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c16", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c17", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c18", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c19", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c20", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c21", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c22", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c23", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c24", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c25", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c26", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c27", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c28", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c29", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c30", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c31", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c32", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c33", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c34", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c35", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c36", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c37", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c38", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("c39", new VarCharType(VarCharType.MAX_LENGTH))
    ));
    public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE)
        );

    public static Row randomRow(int id) {
        return Row.of(
            id,
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString(),
            randomString()
        );
    }

    private static String randomString() {
        return RandomStringUtils.random(10);
    }

}
