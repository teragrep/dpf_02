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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;

/**
 * Lexicographical string sort.
 */
public final class IpAddressSort implements SortMethod, Serializable {
    private final String columnName;
    private final boolean descending;

    public IpAddressSort(final String columnName, final boolean descending) {
        this.columnName = columnName;
        this.descending = descending;
    }

    private Comparator<Row> comparator() {
        return (r0, r1) -> {
            final long l0 = addrToLong(r0.getAs(r0.fieldIndex(columnName)));
            final long l1 = addrToLong(r1.getAs(r1.fieldIndex(columnName)));
            int comp = Long.compare(l0, l1);

            if (descending) {
                comp = -1 * comp;
            }

            return comp;
        };
    }

    private long addrToLong(String ip) {
        InetAddress ip1;
        long ip_addr1 = 0L;

        try {
            ip1 = InetAddress.getByName(ip);

        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Error converting IP address into comparable format: ", e);
        }

        for (byte b : ip1.getAddress()) {
            ip_addr1 = ip_addr1 << 8 | (b & 0xFF);
        }

        return ip_addr1;
    }

    @Override
    public List<Row> sort(final List<Row> rows) {
        if (!rows.isEmpty()) {
            final Row first = rows.get(0);

            try {
                first.fieldIndex(columnName);
            } catch (IllegalArgumentException e) {
                return rows;
            }
        }
        rows.sort(comparator());
        return rows;
    }
}
