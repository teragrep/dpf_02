package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface SearchResults {

    public abstract SearchResults including(final Dataset<Row> batch, Limits limits);

    public abstract Dataset<Row> dataset();

}
