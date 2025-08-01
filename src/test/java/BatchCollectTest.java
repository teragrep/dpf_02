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

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.functions.dpf_02.BatchCollect;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class BatchCollectTest {

    // see https://stackoverflow.com/questions/56894068/how-to-perform-unit-testing-on-spark-structured-streaming
    // see ./sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamingJoinSuite.scala at 2.4.5

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
    public void testCollectAsDataframe() {
    	SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        BatchCollect batchCollect = new BatchCollect("_time", 100);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        StreamingQuery streamingQuery = Assertions.assertDoesNotThrow(() -> startStream(rowDataset, batchCollect, false, Collections.emptyList()));

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

            // create 20 events for 10 runs
            if (counter == 20) {
                run++;
                counter = 0;
            }
            counter++;

            if (run == 10) {
                // 10 runs only
                // wait until the source feeds them all?
                // TODO there must be a better way?
                streamingQuery.processAllAvailable();
				Assertions.assertDoesNotThrow(streamingQuery::stop);
				Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
            }
        }

        Dataset<Row> collectedAsDF = batchCollect.getCollectedAsDataframe();
        Assertions.assertEquals(100, collectedAsDF.count());

        // assert that batches are correct (the newest 100 rows of data)
        // batch number 3 is the newest in the test, others are in the order of creation
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("3")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("6")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("7")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("8")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("9")).count());
    }

    @Test
    public void testSkipLimiting() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        BatchCollect batchCollect = new BatchCollect("_time", 5);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();

        // Skip limiting here
        StreamingQuery streamingQuery = Assertions.assertDoesNotThrow(() -> startStream(rowDataset, batchCollect, true, Collections.emptyList()));

        long run = 0;
        long counter = 0;
        while (streamingQuery.isActive()) {
            Timestamp time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            if (run == 3) {
                // make run 3 to be latest always
                time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(13851486065L+counter), ZoneOffset.UTC));
            } else if (run == 10) {
                // 10 runs only
                streamingQuery.processAllAvailable();
                Assertions.assertDoesNotThrow(streamingQuery::stop);
                Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
                break;
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

            counter++;

            // create 20 events for 10 runs
            if (counter == 20) {
                run++;
                counter = 0;
            }
        }

        Dataset<Row> collectedAsDF = batchCollect.getCollectedAsDataframe();

        // all the rows in the dataset, the limit of 5 rows is therefore not applied
        Assertions.assertEquals(200, collectedAsDF.count());

        // assert that batches are correct (all the rows, 10 batches)
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("0")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("1")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("2")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("3")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("4")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("5")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("6")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("7")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("8")).count());
        Assertions.assertEquals(20, collectedAsDF.filter(functions.col("partition").equalTo("9")).count());
    }

    @Test
    public void testPostBatchCollectStepProcessing() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        BatchCollect batchCollect = new BatchCollect("_time", 100);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        StreamingQuery streamingQuery = Assertions.assertDoesNotThrow(() -> startStream(rowDataset, batchCollect, false,
                Collections.singletonList(new TestAggregationStep())));

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

            // create 20 events for 10 runs
            if (counter == 20) {
                run++;
                counter = 0;
            }
            counter++;

            if (run == 10) {
                // 10 runs only
                // wait until the source feeds them all?
                // TODO there must be a better way?
                streamingQuery.processAllAvailable();
                Assertions.assertDoesNotThrow(streamingQuery::stop);
                Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
            }
        }

        Dataset<Row> collectedAsDF = batchCollect.getCollectedAsDataframe();
        Assertions.assertEquals(1, collectedAsDF.count());

        Assertions.assertEquals(201L, collectedAsDF.first().getLong(0));
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


    private StreamingQuery startStream(Dataset<Row> rowDataset, BatchCollect batchCollect,
                                       boolean skipLimiting, List<AbstractStep> steps) throws TimeoutException {
        return rowDataset
                .writeStream()
                .foreachBatch(
                        (VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> {
                            batchCollect.collect(batchDF, batchId, steps, skipLimiting);
                        }
                )
                .outputMode("append")
                .start();
    }
}
