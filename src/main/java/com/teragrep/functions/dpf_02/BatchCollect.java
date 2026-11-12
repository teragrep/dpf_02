package com.teragrep.functions.dpf_02;

/*
 * Teragrep Batch Collect DPF-02
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class BatchCollect {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollect.class);

    private final SearchResults emptyResults;
    private final int batchLimit;
    private final int resultsLimit;
    private final int postBatchCollectLimit;
    private SearchResults results;
    private Dataset<Row> outputDs;
    private Dataset<Row> lastRowDs;

    public BatchCollect(final String defaultColumn, final int batchLimit) {
        this(defaultColumn, batchLimit, 0, Collections.emptyList());
    }

    public BatchCollect(final String defaultColumn, final int batchLimit, final int postBatchCollectLimit, final List<SortByClause> sortByClauses) {
        this(new Sort(sortByClauses, defaultColumn), batchLimit, batchLimit, postBatchCollectLimit);
    }

    public BatchCollect(final Sort sort, final int batchLimit, int resultsLimit, int postBatchCollectLimit) {
        this(new EmptyResults(sort), batchLimit, resultsLimit, postBatchCollectLimit);
    }


    public BatchCollect(final SearchResults initialResults, int batchLimit, int resultsLimit, int postBatchCollectLimit) {
        this(initialResults, initialResults, batchLimit, resultsLimit, postBatchCollectLimit);
    }

    public BatchCollect(final SearchResults initialResults, final SearchResults results, int batchLimit, int resultsLimit, int postBatchCollectLimit) {
        this.emptyResults = initialResults;
        this.results = results;
        this.batchLimit = batchLimit;
        this.resultsLimit = resultsLimit;
        this.postBatchCollectLimit = postBatchCollectLimit;
    }

    /**
     * Returns the batchCollected dataframe.
     * @param df  Dataframe to perform batch collection (and sorting) on
     * @param id  identification number for the dataframe, usually batch ID
     * @param skipLimiting Skips limiting of initial dataset, use for aggregated datasets
     * @return sorted dataset
     */
    public Dataset<Row> call(Dataset<Row> df, Long id, boolean skipLimiting) {
        Dataset<Row> rv;
        collect(df, id, Collections.emptyList(), skipLimiting);

        if (lastRowDs != null) {
            rv = outputDs.union(lastRowDs);
        } else {
            rv = outputDs;
        }

        return rv;
    }

    public void collect(Dataset<Row> batchDF, Long batchId, List<AbstractStep> postBcSteps, boolean skipLimiting) {
        LOGGER.trace("collect() processing batch id <{}> with skip limiting <{}>", batchId, skipLimiting);

        final int effectiveResultsLimit = postBcSteps.isEmpty()
                ? resultsLimit
                : postBatchCollectLimit;

        final Limits collectionLimit;

        if (skipLimiting || effectiveResultsLimit < 1) {
            collectionLimit = new Limits();
        } else {
            int selectedBatchLimit = Math.max(batchLimit, effectiveResultsLimit);
            collectionLimit = new Limits(selectedBatchLimit, effectiveResultsLimit);
        }

        final SearchResults accumulatedResults = results.including(batchDF, collectionLimit);
        final SearchResults postProcessedResults = new PostBatchCollectResults(accumulatedResults, postBcSteps);

        this.results = accumulatedResults;
        this.outputDs = postProcessedResults.dataset();
    }

    private Dataset<Row> orderDataset(Dataset<Row> ds) {
        return results.dataset();
    }

    public Dataset<Row> getCollectedAsDataframe() {
        Dataset<Row> rv;
        if (this.lastRowDs != null) {
            rv = this.outputDs.union(this.lastRowDs);
        } else {
            rv = this.outputDs;
        }
        Objects.requireNonNull(rv, "getCollectedAsDataframe() tried to return null value");
        return rv;
    }

    public void clear() {
        LOGGER.info("dpf_02 cleared");
        this.results = emptyResults;
        this.outputDs = null;
        this.lastRowDs = null;
    }

    public void updateLastRow(Dataset<Row> lastRow) {
        this.lastRowDs = lastRow;
    }
}
