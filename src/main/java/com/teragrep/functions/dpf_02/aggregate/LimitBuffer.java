package com.teragrep.functions.dpf_02.aggregate;
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
import com.teragrep.functions.dpf_02.operation.limit.LimitedList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class LimitBuffer implements Serializable, BufferTrait {
    private final List<Row> rows;
    private final int count;
    private final int limit;

    public LimitBuffer(final int limit) {
        this(new ArrayList<>(), 0, limit);
    }

    public LimitBuffer(final List<Row> rows, final int count, final int limit) {
        this.rows = rows;
        this.count = count;
        this.limit = limit;
    }

    @Override
    public BufferTrait zero() {
        return new LimitBuffer(new ArrayList<>(), 0, limit);
    }

    public LimitBuffer reduce(final Row input) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.add(input);

        final int newCount = count + 1;

        if (newCount > limit) {
            newRows = new LimitedList<>(limit, newRows).toList();
        }

        return new LimitBuffer(newRows, newCount, limit);
    }

    @Override
    public BufferTrait merge(final BufferTrait another) {
        List<Row> merged = another.toList();
        merged.addAll(rows);

        int newCount = count + another.count();

        if (newCount > limit) {
            merged = new LimitedList<>(limit, merged).toList();
            newCount = limit;
        }

        return new LimitBuffer(merged, newCount, limit);
    }

    @Override
    public Row finish() {
        List<Row> rv = rows;
        if (count > limit) {
            rv = new LimitedList<>(limit, rv).toList();
        }
        return RowFactory.create(JavaConverters.asScalaBuffer(rv).toSeq());
    }

    @Override
    public List<Row> toList() {
        return new ArrayList<>(rows);
    }

    @Override
    public int count() {
        return count;
    }
}
