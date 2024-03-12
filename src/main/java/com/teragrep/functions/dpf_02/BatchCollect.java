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
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class BatchCollect extends SortOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollect.class);
    private Dataset<Row> savedDs = null;
    private final String sortColumn;
    private final int numberOfRows;
    private StructType inputSchema;
    private boolean sortedBySingleColumn = false;

    public BatchCollect(String sortColumn, int numberOfRows) {
        super();

        LOGGER.info("Initialized BatchCollect based on column " + sortColumn + " and a limit of " + numberOfRows
                + " row(s)");
        this.sortColumn = sortColumn;
        this.numberOfRows = numberOfRows;
    }

    public BatchCollect(String sortColumn, int numberOfRows, List<SortByClause> listOfSortByClauses) {
        super(listOfSortByClauses);

        LOGGER.info("Initialized BatchCollect based on column " + sortColumn + " and a limit of " + numberOfRows + " row(s)." +
                " SortByClauses included: " + (listOfSortByClauses != null ? listOfSortByClauses.size() : "<null>"));
        this.sortColumn = sortColumn;
        this.numberOfRows = numberOfRows;
    }

    /**
     * Returns the batchCollected dataframe.
     * @param df  Dataframe to perform batch collection (and sorting) on
     * @param id  identification number for the dataframe, usually batch ID
     * @param skipLimiting Skips limiting of initial dataset, use for aggregated datasets
     * @return sorted dataset
     */
    public Dataset<Row> call(Dataset<Row> df, Long id, boolean skipLimiting) {
        if (skipLimiting) {
            this.processAggregated(df);
        }
        else {
            this.collect(df, id);
        }

        return this.savedDs;
    }

    public void collect(Dataset<Row> batchDF, Long batchId) {
        // check that sortColumn (_time) exists,
        // and get the sortColId
        // otherwise, no sorting will be done.
        if (this.inputSchema == null) {
            this.inputSchema = batchDF.schema();
        }

        if (this.getListOfSortByClauses() == null || this.getListOfSortByClauses().size() < 1) {
            for (String field : this.inputSchema.fieldNames()) {
                if (field.equals(this.sortColumn)) {
                    this.sortedBySingleColumn = true;
                    break;
                }
            }
        }

        // sort dataset and limit
        Dataset<Row> orderedAndLimitedDs = orderDataset(batchDF).limit(numberOfRows);

        // union with previous data
        if (this.savedDs == null) {
            this.savedDs = orderedAndLimitedDs;
        }
        else {
            this.savedDs = savedDs.union(orderedAndLimitedDs);
        }

        this.savedDs = orderDataset(this.savedDs).limit(numberOfRows);
    }

    // Call this instead of collect to skip limiting (for aggregatesUsed=true)
    // TODO remove this
    public void processAggregated(Dataset<Row> ds) {
        if (this.inputSchema == null) {
            this.inputSchema = ds.schema();
        }

        List<Row> collected = orderDataset(ds).limit(numberOfRows).collectAsList();
        Dataset<Row> createdDsFromCollected = SparkSession.builder().getOrCreate().createDataFrame(collected, this.inputSchema);

        if (this.savedDs == null) {
            this.savedDs = createdDsFromCollected;
        }
        else {
            this.savedDs = savedDs.union(createdDsFromCollected);
        }

        this.savedDs = orderDataset(this.savedDs).limit(numberOfRows);
    }

    private Dataset<Row> orderDataset(Dataset<Row> ds) {
        if (this.sortedBySingleColumn) {
            return ds.orderBy(functions.col(this.sortColumn).desc());
        } else {
            return this.orderDatasetByGivenColumns(ds);
        }
    }

    // TODO: Remove
    public List<Row> getCollected() {
        return this.savedDs.collectAsList();
    }

    public Dataset<Row> getCollectedAsDataframe() {
        return this.savedDs;
    }

    public void clear() {
        LOGGER.info("dpf_02 cleared");
        this.savedDs = null;
        this.inputSchema = null;
    }
}
