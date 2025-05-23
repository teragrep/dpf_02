package com.teragrep.functions.dpf_02.operation;

import org.apache.spark.sql.Row;

import java.util.Comparator;
import java.util.List;

public class SortingOperation implements RowOperation {
    private final String columnName;
    private final Type type;
    private final Order order;

    public enum Type {
        DEFAULT, STRING, NUMERIC, IP_ADDRESS, AUTOMATIC, TIMESTAMP
    }

    public enum Order {
        ASCENDING, DESCENDING
    }

    public SortingOperation(final String columnName, final Type type, final Order order) {
        this.columnName = columnName;
        this.type = type;
        this.order = order;
    }

    @Override
    public List<Row> apply(final List<Row> rows) {
        final Comparator<Row> sortMethod;
        if (type == Type.TIMESTAMP) {
            sortMethod = new TimestampSort(columnName, order.equals(Order.DESCENDING));
        } else if (type == Type.STRING) {
            sortMethod = new StringSort(columnName, order.equals(Order.DESCENDING));
        } else if (type == Type.NUMERIC) {
            sortMethod = new NumericSort(columnName, order.equals(Order.DESCENDING));
        } else if (type == Type.IP_ADDRESS) {
            sortMethod = new IpAddressSort(columnName, order.equals(Order.DESCENDING));

        } else {
            throw new UnsupportedOperationException("Unsupported sort type: " + type);
        }

        rows.sort(sortMethod);
        return rows;
    }
}
