package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.List;

final class PostBatchCollectResults implements SearchResults {
    private final SearchResults origin;
    private final List<AbstractStep> steps;

    PostBatchCollectResults(final SearchResults origin, final List<AbstractStep> steps) {
        this.origin = origin;
        this.steps = List.copyOf(steps);
    }

    @Override
    public SearchResults including(final Dataset<Row> batch, final Limits limits) {
        return new PostBatchCollectResults(
                origin.including(batch, limits),
                steps
        );
    }

    @Override
    public Dataset<Row> dataset() {
        Dataset<Row> result = origin.dataset();
        for (final AbstractStep step : steps) {
            try {
                result = step.get(result);
            } catch (final StreamingQueryException e) {
                throw new IllegalStateException("Exception occurred while running post-batchcollect steps <" + e.getMessage() + ">", e);
            }
        }
        return result;
    }
}
