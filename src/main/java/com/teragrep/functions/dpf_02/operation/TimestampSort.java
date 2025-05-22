package com.teragrep.functions.dpf_02.operation;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;

public class TimestampSort implements Comparator<Row>, Serializable {
    private final String columnName;
    private final boolean descending;

    public TimestampSort() {
        this("_time", true);
    }

    public TimestampSort(final boolean descending) {
        this("_time", descending);
    }

    public TimestampSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    @Override
    public int compare(final Row r0, final Row r1) {
        final Timestamp t0 = r0.getTimestamp(r0.fieldIndex(columnName));
        final Timestamp t1 = r1.getTimestamp(r1.fieldIndex(columnName));
        int comp = t0.compareTo(t1);

        if (descending) {
            comp = -1 * comp;
        }

        return comp;
    }
}
