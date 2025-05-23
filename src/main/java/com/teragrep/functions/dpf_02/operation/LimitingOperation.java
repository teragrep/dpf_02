package com.teragrep.functions.dpf_02.operation;

import org.apache.spark.sql.Row;

import java.util.List;

public class LimitingOperation implements RowOperation {
    private final int count;

    public LimitingOperation(final int count) {
        this.count = count;
    }

    private void validate() {
        if (count < 0) {
            throw new IllegalArgumentException("Limit must be greater than or equal to zero, was: " + count);
        }
    }

    @Override
    public List<Row> apply(final List<Row> rows) {
        validate();
        return rows.subList(0, Math.min(count, rows.size()));
    }

}
