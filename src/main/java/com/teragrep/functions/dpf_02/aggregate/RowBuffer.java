package com.teragrep.functions.dpf_02.aggregate;

import com.teragrep.functions.dpf_02.operation.RowOperation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class RowBuffer implements Serializable {
    private final List<Row> rows;
    private final List<RowOperation> rowOps;

    public RowBuffer() {
        this(new ArrayList<>(), new ArrayList<>());
    }

    public RowBuffer(List<RowOperation> rowOps) {
        this(new ArrayList<>(), rowOps);
    }

    public RowBuffer(final List<Row> rows, final List<RowOperation> rowOps) {
        this.rows = rows;
        this.rowOps = rowOps;
    }

    public RowBuffer reduce(final Row input) {
        final List<Row> newRows = new ArrayList<>(rows);
        newRows.add(input);
        return new RowBuffer(newRows, rowOps);
    }

    public RowBuffer merge(final RowBuffer another) {
        final List<Row> merged = another.toList();
        merged.addAll(rows);
        return new RowBuffer(merged, rowOps);
    }

    public Row finish() {
        List<Row> rv = rows;
        for (final RowOperation op : rowOps) {
            rv = op.apply(rv);
        }

        return RowFactory.create(JavaConverters.asScalaBuffer(rv).toSeq());
    }

    public List<Row> toList() {
        return new ArrayList<>(rows);
    }
}
