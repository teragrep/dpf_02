package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class Limits {

    private final int batchLimit;
    private final int resultsLimit;

    public Limits() {
        this(0, 0);
    }

    public Limits(final int batchLimit, final int resultsLimit) {
        this.batchLimit = batchLimit;
        this.resultsLimit = resultsLimit;
    }

    public Dataset<Row> incoming(final Dataset<Row> dataset) {
        final Dataset<Row> result;

        if (batchLimit > 0) {
            result = dataset.limit(batchLimit);
        } else {
            result = dataset;
        }
        return result;
    }

    public Dataset<Row> accumulated(final Dataset<Row> dataset) {
        final Dataset<Row> result;
        if (resultsLimit > 0) {
            result = dataset.limit(resultsLimit);
        } else {
            result = dataset;
        }
        return result;
    }
}
