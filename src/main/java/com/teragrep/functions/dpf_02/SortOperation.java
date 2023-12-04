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

import java.util.ArrayList;
import java.util.List;

public abstract class SortOperation {
    private List<SortByClause> listOfSortByClauses = null;

    /**
     * Initializes an empty list of SortByClauses
     */
    public SortOperation() {
        this.listOfSortByClauses = new ArrayList<>();
    }

    public SortOperation(List<SortByClause> listOfSortByClauses) {
        this.listOfSortByClauses = listOfSortByClauses;
    }

    public List<SortByClause> getListOfSortByClauses() {
        return this.listOfSortByClauses;
    }

    public void addSortByClause(SortByClause sortByClause) {
        this.listOfSortByClauses.add(sortByClause);
    }

    // Performs orderBy operation on a dataset and returns the ordered one
    public Dataset<Row> orderDatasetByGivenColumns(Dataset<Row> ds) {
        final SparkSession ss = SparkSession.builder().getOrCreate();

        if (this.listOfSortByClauses != null && this.listOfSortByClauses.size() > 0) {
            for (SortByClause sbc : listOfSortByClauses) {
                if (sbc.getSortAsType() == SortByClause.Type.AUTOMATIC) {
                    SortByClause.Type autoType = detectSortByType(ds, sbc.getFieldName());
                    ds = orderDatasetBySortByClause(ss, ds, sbc, autoType);
                }
                else {
                    ds = orderDatasetBySortByClause(ss, ds, sbc, null);
                }
            }
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
    private SortByClause.Type detectSortByType(final Dataset<Row> ds, final String fieldName) {
        StructField[] fields = ds.schema().fields();
        for (StructField field : fields) {
            if (field.name().equals(fieldName)) {
                switch (field.dataType().typeName()) {
                    case "string": // ip address?
                        return numericalStringCheck(ds, fieldName);
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

    /**
     * Checks if a column only contains numbers even if it is labeled as a string column.
     * @param dataset dataset to check
     * @param fieldName name of the column
     * @return Numeric or String SortByClause.Type
     */
    private SortByClause.Type numericalStringCheck(Dataset<Row> dataset, String fieldName) {
        // Value is numerical if it is castable to Double
        Dataset<Row> tempDataset = dataset.withColumn("isNumerical", functions.col(fieldName).cast(DataTypes.DoubleType).isNotNull());

        // If the isNumerical column has even one false value, the column contains strings
        if (tempDataset.filter(tempDataset.col("isNumerical").contains(false)).isEmpty()) {
            // no false values found
            return SortByClause.Type.NUMERIC;
        } else {
            return SortByClause.Type.STRING;
        }
    }
}