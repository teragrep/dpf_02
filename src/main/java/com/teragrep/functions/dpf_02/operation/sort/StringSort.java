package com.teragrep.functions.dpf_02.operation.sort;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Lexicographical string sort.
 */
public final class StringSort implements SortMethod, Serializable {
    private final String columnName;
    private final boolean descending;

    public StringSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    private Comparator<Row> comparator() {
        return (r0, r1) -> {
            final Object o0 = r0.getAs(r0.fieldIndex(columnName));
            final Object o1 = r1.getAs(r1.fieldIndex(columnName));
            int comp = o0.toString().compareTo(o1.toString());

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
