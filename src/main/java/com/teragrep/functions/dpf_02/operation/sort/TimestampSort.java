package com.teragrep.functions.dpf_02.operation.sort;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;

/**
 * Timestamp-based sort.
 */
public final class TimestampSort implements SortMethod, Serializable {
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

    private Comparator<Row> comparator() {
        return (r0, r1) -> {
            final Timestamp t0 = r0.getTimestamp(r0.fieldIndex(columnName));
            final Timestamp t1 = r1.getTimestamp(r1.fieldIndex(columnName));
            int comp = t0.compareTo(t1);

            if (descending) {
                comp = -1 * comp;
            }

            return comp;
        };
    }

    @Override
    public List<Row> sort(final List<Row> rows) {
        rows.sort(comparator());
        return rows;
    }
}
