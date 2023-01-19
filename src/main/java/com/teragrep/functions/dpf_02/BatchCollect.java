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
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class BatchCollect {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollect.class);
    private Dataset<Row> savedDs = null;
    private final String sortColumn;
    private final int numberOfRows;
    private StructType inputSchema;
    private boolean sortedBySingleColumn = false;
    private List<SortByClause> listOfSortByClauses = null;

    public BatchCollect(String sortColumn, int numberOfRows) {
        LOGGER.info("Initialized BatchCollect based on column " + sortColumn + " and a limit of " + numberOfRows
                + " row(s)");
        this.sortColumn = sortColumn;
        this.numberOfRows = numberOfRows;
    }

    public BatchCollect(String sortColumn, int numberOfRows, List<SortByClause> listOfSortByClauses) {
        LOGGER.info("Initialized BatchCollect based on column " + sortColumn + " and a limit of " + numberOfRows + " row(s)." +
                " SortByClauses included: " + (listOfSortByClauses != null ? listOfSortByClauses.size() : "<null>"));
        this.sortColumn = sortColumn;
        this.numberOfRows = numberOfRows;

        this.listOfSortByClauses = listOfSortByClauses;
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

        if (this.listOfSortByClauses == null || this.listOfSortByClauses.size() < 1) {
            for (String field : this.inputSchema.fieldNames()) {
                if (field.equals(this.sortColumn)) {
                    this.sortedBySingleColumn = true;
                    break;
                }
            }
        }

        List<Row> collected = orderDatasetByGivenColumns(batchDF).limit(numberOfRows).collectAsList();
        Dataset<Row> createdDsFromCollected = SparkSession.builder().getOrCreate().createDataFrame(collected, this.inputSchema);

        if (this.savedDs == null) {
            this.savedDs = createdDsFromCollected;
        }
        else {
            this.savedDs = savedDs.union(createdDsFromCollected);
        }

        this.savedDs = orderDatasetByGivenColumns(this.savedDs).limit(numberOfRows);

    }

    // Call this instead of collect to skip limiting (for aggregatesUsed=true)
    // TODO remove this
    public void processAggregated(Dataset<Row> ds) {
        if (this.inputSchema == null) {
            this.inputSchema = ds.schema();
        }

        List<Row> collected = orderDatasetByGivenColumns(ds).limit(numberOfRows).collectAsList();
        Dataset<Row> createdDsFromCollected = SparkSession.builder().getOrCreate().createDataFrame(collected, this.inputSchema);

        if (this.savedDs == null) {
            this.savedDs = createdDsFromCollected;
        }
        else {
            this.savedDs = savedDs.union(createdDsFromCollected);
        }

        this.savedDs = orderDatasetByGivenColumns(this.savedDs).limit(numberOfRows);
    }

    // Performs orderBy operation on a dataset and returns the ordered one
    private Dataset<Row> orderDatasetByGivenColumns(Dataset<Row> ds) {
        final SparkSession ss = SparkSession.builder().getOrCreate();

        if (this.listOfSortByClauses != null && this.listOfSortByClauses.size() > 0) {
            for (SortByClause sbc : listOfSortByClauses) {
                if (sbc.getSortAsType() == SortByClause.Type.AUTOMATIC) {
                    SortByClause.Type autoType = detectSortByType(ds.schema().fields(), sbc.getFieldName());
                    ds = orderDatasetBySortByClause(ss, ds, sbc, autoType);
                }
                else {
                    ds = orderDatasetBySortByClause(ss, ds, sbc, null);
                }
            }
        }
        else if (this.sortedBySingleColumn) {
            ds = ds.orderBy(functions.col(this.sortColumn).desc());
        }

        return ds;
    }

    // orderBy based on sortByClause type and if it is descending/ascending
    private Dataset<Row> orderDatasetBySortByClause(final SparkSession ss, final Dataset<Row> unsorted, final SortByClause sortByClause, final SortByClause.Type overrideSortType) {
        Dataset<Row> rv = null;
        SortByClause.Type sortByType = sortByClause.getSortAsType();
        if (overrideSortType != null) {
            sortByType = overrideSortType;
        }

        switch (sortByType) {
            case DEFAULT:
                rv = unsorted.orderBy(sortByClause.isDescending() ?
                        functions.col(sortByClause.getFieldName()).desc() :
                        functions.col(sortByClause.getFieldName()).asc());
                break;
            case STRING:
                rv = unsorted.orderBy(sortByClause.isDescending() ?
                        functions.col(sortByClause.getFieldName()).cast(DataTypes.StringType).desc() :
                        functions.col(sortByClause.getFieldName()).cast(DataTypes.StringType).asc());
                break;
            case NUMERIC:
                rv = unsorted.orderBy(sortByClause.isDescending() ?
                        functions.col(sortByClause.getFieldName()).cast(DataTypes.DoubleType).desc() :
                        functions.col(sortByClause.getFieldName()).cast(DataTypes.DoubleType).asc());
                break;
            case IP_ADDRESS:
                UserDefinedFunction ipStringToIntUDF = functions.udf(new ConvertIPStringToInt(), DataTypes.LongType);
                ss.udf().register("ip_string_to_int", ipStringToIntUDF);
                Column sortingCol = functions.callUDF("ip_string_to_int", functions.col(sortByClause.getFieldName()));

                rv = unsorted.orderBy(sortByClause.isDescending() ? sortingCol.desc() : sortingCol.asc());
                break;
        }
        return rv;
    }

    // detect sorting type if auto() was used in sort
    private SortByClause.Type detectSortByType(final StructField[] fields, final String fieldName) {
        for (StructField field : fields) {
            if (field.name().equals(fieldName)) {
                switch (field.dataType().typeName()) {
                    case "string": // ip address?
                        return SortByClause.Type.STRING;
                    case "long":
                    case "integer":
                    case "float":
                    case "double":
                        return SortByClause.Type.NUMERIC;
                    case "timestamp":
                        return SortByClause.Type.NUMERIC; // convert to unix epoch?
                    default:
                        return SortByClause.Type.DEFAULT;
                }
            }
        }
        return SortByClause.Type.DEFAULT;
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
