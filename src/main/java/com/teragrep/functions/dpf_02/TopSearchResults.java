package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Objects;

public final class TopSearchResults implements SearchResults {
    private final Sort sort;
    private final List<Row> retained;
    private final StructType schema;
    private final SparkSession spark;

    public TopSearchResults(
            final Sort sort,
            final List<Row> retained,
            final StructType schema,
            final SparkSession spark
    ) {
        this.sort = Objects.requireNonNull(sort);
        this.retained = List.copyOf(Objects.requireNonNull(retained));
        this.schema = Objects.requireNonNull(schema);
        this.spark = Objects.requireNonNull(spark);
    }

    @Override
    public SearchResults including(final Dataset<Row> batch, final Limits limits) {
        if (!schema.equals(batch.schema())) {
            throw new IllegalArgumentException("Incoming batch schema differs from retained results");
        }
        final Dataset<Row> sortedBatch = sort.apply(batch);
        final Dataset<Row> limitedBatch = limits.incoming(sortedBatch);
        final List<Row> incomingRows = limitedBatch.collectAsList();
        final Dataset<Row> existing = spark.createDataFrame(retained, schema);
        final Dataset<Row> additions = spark.createDataFrame(incomingRows, schema);
        final List<Row> selected = sort.apply(existing.union(additions)).collectAsList();
        return new TopSearchResults(
                sort,
                selected,
                schema,
                spark
        );
    }

    @Override
    public Dataset<Row> dataset() {
        final Dataset<Row> dataFrame = spark.createDataFrame(retained, schema);
        return sort.apply(dataFrame);
    }
}
