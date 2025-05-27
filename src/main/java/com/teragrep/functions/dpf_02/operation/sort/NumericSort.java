package com.teragrep.functions.dpf_02.operation.sort;
/*
 * Teragrep Batch Collect DPF-02
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Lexicographical string sort.
 */
public final class NumericSort implements SortMethod, Serializable {
    private final String columnName;
    private final boolean descending;

    public NumericSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    private Comparator<Row> comparator() {
        return (r0, r1) -> {
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
        };
    }

    @Override
    public List<Row> sort(final List<Row> rows) {
        rows.sort(comparator());
        return rows;
    }
}
