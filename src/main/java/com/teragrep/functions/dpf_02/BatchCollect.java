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
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class BatchCollect extends SortOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollect.class);
    private Dataset<Row> savedDs = null;
    private Dataset<Row> lastRowDs = null;
    private Dataset<Row> outputDs = null;
    private final String sortColumn;
    private final int defaultLimit;
    private final int postBcLimit;
    private StructType inputSchema;
    private boolean sortedBySingleColumn = false;

    public BatchCollect(String sortColumn, int defaultLimit) {
        this(sortColumn, defaultLimit, new ArrayList<>());
    }

    public BatchCollect(String sortColumn, int defaultLimit, List<SortByClause> listOfSortByClauses) {
        this(sortColumn, defaultLimit, listOfSortByClauses, 0);
    }

    public BatchCollect(String sortColumn, int defaultLimit, List<SortByClause> listOfSortByClauses, int postBcLimit) {
        super(listOfSortByClauses);
        LOGGER.info("Initialized BatchCollect based on column <[{}]> and a limit of <[{}]> row(s). SortByClauses included: <[{}]>. Post batchcollect limit of <[{}]> row(s)",
                sortColumn, defaultLimit, (listOfSortByClauses != null ? listOfSortByClauses.size() : "null"), postBcLimit);
        this.sortColumn = sortColumn;
        this.defaultLimit = defaultLimit;
        this.postBcLimit = postBcLimit;
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
        this.collect(df, id, Collections.emptyList(), skipLimiting);

        if (this.lastRowDs != null) {
            rv = this.outputDs.union(this.lastRowDs);
        } else {
            rv = this.outputDs;
        }

        return rv;
    }

    public void collect(Dataset<Row> batchDF, Long batchId, List<AbstractStep> postBcSteps, boolean skipLimiting) {
        // Apply post-batchcollect limit if steps are present, otherwise use the default.
        // limit<=0 means no limit
        final int limit;
        if (!postBcSteps.isEmpty()) {
            limit = this.postBcLimit;
        } else {
            limit = this.defaultLimit;
        }

        // check that sortColumn (_time) exists,
        // and get the sortColId
        // otherwise, no sorting will be done.
        if (this.inputSchema == null) {
            this.inputSchema = batchDF.schema();
        }

        if (this.getListOfSortByClauses() == null || this.getListOfSortByClauses().isEmpty()) {
            for (String field : this.inputSchema.fieldNames()) {
                if (field.equals(this.sortColumn)) {
                    this.sortedBySingleColumn = true;
                    break;
                }
            }
        }

        Dataset<Row> orderedDs = orderDataset(batchDF);
        if (!skipLimiting && limit > 0) {
            orderedDs = orderedDs.limit(limit);
        }
        List<Row> collected = orderedDs.collectAsList();
        Dataset<Row> createdDsFromCollected = SparkSession.builder().getOrCreate().createDataFrame(collected, this.inputSchema);
        Dataset<Row> current;
        if (this.savedDs == null) {
            current = createdDsFromCollected;
        }
        else {
            current = savedDs.union(createdDsFromCollected);
        }

        current = orderDataset(current);
        if (!skipLimiting && limit > 0) {
            current = current.limit(limit);
        }
        this.savedDs = current;

        // Post batchCollect steps processing
        Dataset<Row> rv = current;
        for (final AbstractStep step : postBcSteps) {
            try {
                rv = step.get(rv);
            } catch (StreamingQueryException e) {
                throw new IllegalStateException("Exception occurred while running post-batchcollect steps: ", e);
            }
        }

        this.outputDs = rv;
    }

    private Dataset<Row> orderDataset(Dataset<Row> ds) {
        if (this.sortedBySingleColumn) {
            return ds.orderBy(functions.col(this.sortColumn).desc());
        } else {
            return this.orderDatasetByGivenColumns(ds);
        }
    }

    public Dataset<Row> getCollectedAsDataframe() {
        Dataset<Row> rv;
        if (this.lastRowDs != null) {
           rv = this.outputDs.union(this.lastRowDs);
        } else {
            rv = this.outputDs;
        }
        return rv;
    }

    public void clear() {
        LOGGER.info("dpf_02 cleared");
        this.savedDs = null;
        this.outputDs = null;
        this.lastRowDs = null;
        this.inputSchema = null;
    }

    public void updateLastRow(Dataset<Row> lastRow) {
        this.lastRowDs = lastRow;
    }
}
