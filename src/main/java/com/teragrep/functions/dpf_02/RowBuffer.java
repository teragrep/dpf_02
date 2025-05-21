package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class RowBuffer implements Serializable {
    private final List<Row> rows;

    public RowBuffer() {
        this(new ArrayList<>());
    }

    public RowBuffer(final List<Row> rows) {
        this.rows = rows;
    }

    public RowBuffer reduce(final Row input) {
        final List<Row> newRows = new ArrayList<>(rows);
        newRows.add(input);
        return new RowBuffer(newRows);
    }

    public RowBuffer merge(final RowBuffer another) {
        final List<Row> merged = another.toList();
        merged.addAll(rows);
        return new RowBuffer(merged);
    }

    public Row finish() {
        return RowFactory.create(JavaConverters.asScalaBuffer(rows).toSeq());
    }

    public List<Row> toList() {
        return new ArrayList<>(rows);
    }
}
