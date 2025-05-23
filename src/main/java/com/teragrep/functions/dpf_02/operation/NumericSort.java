package com.teragrep.functions.dpf_02.operation;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Lexicographical string sort.
 */
public final class NumericSort implements Comparator<Row>, Serializable {
    private final String columnName;
    private final boolean descending;

    public NumericSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    @Override
    public int compare(final Row r0, final Row r1) {
        // both rows' columns should have the same dataType
        final int i0 = r0.fieldIndex(columnName);
        final int i1 = r1.fieldIndex(columnName);
        final DataType dt0 = r0.schema().apply(i0).dataType();
        final DataType dt1 = r1.schema().apply(i1).dataType();

        if (!dt0.sameType(dt1)) {
            throw new IllegalStateException("The comparable rows have different datatypes for the same column! Column <[" + columnName + "]> expected <[" + dt0 + "]>, got <[" + dt1 + "]>");
        }

        int comp;
        if (dt0.sameType(DataTypes.FloatType)) {
            comp = Float.compare(r0.getFloat(i0), r1.getFloat(i1));
        } else if (dt0.sameType(DataTypes.DoubleType)) {
            comp = Double.compare(r0.getDouble(i0), r1.getDouble(i1));
        } else if (dt0.sameType(DataTypes.IntegerType)) {
            comp = Integer.compare(r0.getInt(i0), r1.getInt(i1));
        } else if (dt0.sameType(DataTypes.LongType)) {
            comp = Long.compare(r0.getLong(i0), r1.getLong(i1));
        } else if (dt0.sameType(DataTypes.ShortType)) {
            comp = Short.compare(r0.getShort(i0), r1.getShort(i1));
        } else if (dt0.sameType(DataTypes.ByteType)) {
            comp = Byte.compare(r0.getByte(i0), r1.getByte(i1));
        } else if (dt0.sameType(DataTypes.TimestampType)) {
            comp = r0.getTimestamp(i0).compareTo(r1.getTimestamp(i1));
        } else {
            // Try to parse into numbers
            String s0 = r0.getAs(i0).toString();
            String s1 = r1.getAs(i1).toString();

            comp = Double.valueOf(s0).compareTo(Double.valueOf(s1));
        }

        if (descending) {
            comp = -1 * comp;
        }

        return comp;
    }
}
