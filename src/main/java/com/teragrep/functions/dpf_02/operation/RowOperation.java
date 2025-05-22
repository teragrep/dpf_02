package com.teragrep.functions.dpf_02.operation;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

public interface RowOperation extends Function<List<Row>, List<Row>>, Serializable {
}
