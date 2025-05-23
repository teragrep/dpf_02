package com.teragrep.functions.dpf_02.operation;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Lexicographical string sort.
 */
public final class StringSort implements Comparator<Row>, Serializable {
    private final String columnName;
    private final boolean descending;

    public StringSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    @Override
    public int compare(final Row r0, final Row r1) {
        final Object o0 = r0.getAs(r0.fieldIndex(columnName));
        final Object o1 = r1.getAs(r1.fieldIndex(columnName));
        int comp = o0.toString().compareTo(o1.toString());

        if (descending) {
            comp = -1 * comp;
        }

        return comp;
    }
}
