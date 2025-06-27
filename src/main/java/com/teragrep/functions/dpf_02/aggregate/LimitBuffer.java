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
import com.teragrep.functions.dpf_02.operation.sort.SortMethod;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;


public final class LimitBuffer implements Serializable, BufferTrait {
    private final List<Row> rows;
    private final int count;
    private final int limit;
    private final List<SortMethod> sortMethods;
    private final TreeMap<Timestamp, Integer> sortMap;

    public LimitBuffer(final int limit) {
        this(new ArrayList<>(), new TreeMap<>(), 0, limit, new ArrayList<>());
    }

    public LimitBuffer(final List<Row> rows,
                       final TreeMap<Timestamp, Integer> sortMap,
                       final int count,
                       final int limit,
                       final List<SortMethod> sortMethods) {
        this.rows = rows;
        this.count = count;
        this.limit = limit;
        this.sortMethods = sortMethods;
        this.sortMap = sortMap;
    }

    @Override
    public BufferTrait zero() {
        return new LimitBuffer(new ArrayList<>(), new TreeMap<>(), 0, limit, sortMethods);
    }

    public LimitBuffer reduce(final Row input) {
        List<Row> rv = rows;
        int newCount = count;

        if (rv.size() >= limit) {
            Map.Entry<Timestamp, Integer> entry = sortMap.firstEntry();
            if (entry.getKey().before(input.getTimestamp(0))) {
                sortMap.remove(entry.getKey());
                rows.set(entry.getValue(), input);
                sortMap.put(input.getTimestamp(0), entry.getValue());
            }
        } else {
            rows.add(input);
            sortMap.put(input.getTimestamp(0), newCount);
            newCount++;
        }

        return new LimitBuffer(rv, sortMap, newCount, limit, sortMethods);
    }

    @Override
    public BufferTrait merge(final BufferTrait another) {
        List<Row> anotherRows = another.toList();
        LimitBuffer current = this;

        for (final Row r : anotherRows) {
            current = current.reduce(r);
        }

        return current;
    }

    @Override
    public Row finish() {
        List<Row> rv = new ArrayList<>(rows.size());

        for (Map.Entry<Timestamp, Integer> entry : sortMap.entrySet()) {
            rv.add(rows.get(entry.getValue()));
        }

        return RowFactory.create(JavaConverters.asScalaBuffer(rv).toSeq());
    }

    @Override
    public List<Row> toList() {
        return rows;
    }

    @Override
    public int count() {
        return count;
    }
}
