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

import com.teragrep.functions.dpf_02.BatchCollect;
import com.teragrep.functions.dpf_02.SortByClause;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class SortOperationTest {

    private static final StructType testSchema = new StructType(
            new StructField[]{
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
            }
    );

    @Test
    public void testMultipleSortByClauses() throws StreamingQueryException, InterruptedException {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, encoder);

        ArrayList<SortByClause> sortByClauses = new ArrayList<>();
        SortByClause partSbc = new SortByClause();
        partSbc.setFieldName("partition");
        partSbc.setDescending(false);
        partSbc.setLimit(20);
        partSbc.setSortAsType(SortByClause.Type.DEFAULT);
        SortByClause offSbc = new SortByClause();
        offSbc.setFieldName("offset");
        offSbc.setDescending(false);
        offSbc.setLimit(20);
        offSbc.setSortAsType(SortByClause.Type.DEFAULT);

        sortByClauses.add(partSbc);
        sortByClauses.add(offSbc);

        BatchCollect batchCollect = new BatchCollect("_time", 20, sortByClauses);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        StreamingQuery streamingQuery = startStream(rowDataset, batchCollect);

        long run = 0;
        long counter = 0;
        while (streamingQuery.isActive()) {
            Timestamp time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            if (run == 3) {
                // make run 3 to be latest always
                time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(13851486065L+counter), ZoneOffset.UTC));
            }

            rowMemoryStream.addData(
                    // make rows containing counter as offset and run as partition
                    makeRows(
                            time,
                            "data data",
                            "topic",
                            "stream",
                            "host",
                            "input",
                            String.valueOf(run),
                            counter,
                            1
                    )
            );

            // create 5 events for 4 runs
            if (counter == 4) {
                run++;
                counter = 0;
            } else {
                counter++;
            }

            if (run == 4) {
                // 4 runs only
                streamingQuery.processAllAvailable();
                streamingQuery.stop();
                streamingQuery.awaitTermination();
            }
        }

        List<Row> partition = batchCollect.getCollectedAsDataframe().select("partition").collectAsList();
        List<Row> offset = batchCollect.getCollectedAsDataframe().select("offset").collectAsList();

        // should be first sorted by partition and then in those partitions sorted by offset
        Assertions.assertEquals("[0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3]",
                Arrays.toString(partition.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals("[0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4]",
                Arrays.toString(offset.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals(20, partition.size());
        Assertions.assertEquals(20, offset.size());
    }

    private Seq<Row> makeRows(Timestamp _time,
                              String _raw,
                              String index,
                              String sourcetype,
                              String host,
                              String source,
                              String partition,
                              Long offset,
                              long amount) {
        ArrayList<Row> rowArrayList = new ArrayList<>();

        Row row = RowFactory.create(
                _time,
                _raw,
                index,
                sourcetype,
                host,
                source,
                partition,
                offset
        );

        while (amount > 0) {
            rowArrayList.add(row);
            amount--;
        }

        Seq<Row> rowSeq = JavaConverters.asScalaIteratorConverter(rowArrayList.iterator()).asScala().toSeq();

        return rowSeq;
    }


    private StreamingQuery startStream(Dataset<Row> rowDataset, BatchCollect batchCollect) {
        return rowDataset
                .writeStream()
                .foreachBatch(
                        new VoidFunction2<Dataset<Row>, Long>() {
                            @Override
                            public void call(Dataset<Row> batchDF, Long batchId) throws Exception {
                                batchCollect.collect(batchDF, batchId);
                            }
                        }
                )
                .outputMode("append")
                .start();
    }
}