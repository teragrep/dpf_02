package com.teragrep.functions.dpf_02.operation.sort;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.util.List;

public class AutomaticSort implements SortMethod {
    private final String columnName;
    private final boolean descending;

    public AutomaticSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    @Override
    public List<Row> sort(final List<Row> rows) {
        if (rows.isEmpty()) {
            return rows;
        }

        Row r = rows.get(0);
        final int i = r.fieldIndex(columnName);
        final DataType dt = r.schema().apply(i).dataType();

        SortMethod sortMethod;
        switch (dt.typeName()) {
            case "long":
            case "integer":
            case "float":
            case "double":
                sortMethod = new NumericSort(columnName, descending);
                break;
            case "timestamp":
                sortMethod = new TimestampSort(columnName, descending);
                break;
            case "string":
            default:
                boolean isNumeric = rows.stream().allMatch(row -> NumberUtils.isCreatable(row.getAs(row.fieldIndex(columnName))));
                if (isNumeric) {
                    sortMethod = new NumericSort(columnName, descending);
                } else {
                    sortMethod = new StringSort(columnName, descending);
                }
        }

        return sortMethod.sort(rows);
    }
}
