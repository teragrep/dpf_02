package com.teragrep.functions.dpf_02.operation.sort;

import org.apache.spark.sql.Row;

import java.util.List;

public interface SortMethod {
    List<Row> sort(List<Row> rows);
}
