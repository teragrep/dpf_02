package com.teragrep.functions.dpf_02.operation.sort;

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
        rows.sort(comparator());
        return rows;
    }
}
