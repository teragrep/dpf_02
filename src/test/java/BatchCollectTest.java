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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

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

    //@Test
    public void testCollect() throws StreamingQueryException, InterruptedException {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, encoder);

        BatchCollect batchCollect = new BatchCollect("_time", 100, null);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        StreamingQuery streamingQuery = startStream(rowDataset, batchCollect);

        long run = 0;
        long counter = 1;
        while (streamingQuery.isActive()) {
            //System.out.println(batchCollect.getCollected().size());

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
            streamingQuery.processAllAvailable();

            if (run == 10) {
                // 10 runs only
                // wait until the source feeds them all?
                // TODO there must be a better way?
//                streamingQuery.processAllAvailable();
                streamingQuery.stop();
				streamingQuery.awaitTermination();
            }
        }


        LinkedList<Integer> runs = new LinkedList<>();
        runs.add(3);
        runs.add(6);
        runs.add(7);
        runs.add(8);
        runs.add(9);
        verifyRuns(batchCollect, runs);
    }
    
    @Test
    public void testCollectAsDataframe() throws StreamingQueryException, InterruptedException {
    	SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, encoder);

        BatchCollect batchCollect = new BatchCollect("_time", 100, null);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        StreamingQuery streamingQuery = startStream(rowDataset, batchCollect);

        long run = 0;
        long counter = 0;
        while (streamingQuery.isActive()) {
            //System.out.println(batchCollect.getCollected().size());

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
				streamingQuery.stop();
				streamingQuery.awaitTermination();
            }
        }
        
        Dataset<Row> collectedAsDF = batchCollect.getCollectedAsDataframe();
        collectedAsDF.show(5, true);
        Assertions.assertTrue(collectedAsDF instanceof Dataset);
        //Assertions.assertEquals(200, collectedAsDF.count());
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

    private void verifyRuns(BatchCollect batchCollect, LinkedList<Integer> runs) {
        // test that 0-4 batches added data to 100 slots
        List<Row> collectedList = batchCollect.getCollected();

        TreeMap<Integer, Long> runToRow = new TreeMap<>();

        int arraySize = collectedList.size();
        while (arraySize != 0) {
            Row row = collectedList.get(arraySize - 1);
            int rowRun = Integer.parseInt(row.getString(6));

            if(runToRow.containsKey(rowRun)) {
                long value = runToRow.get(rowRun);
                value++;
                runToRow.put(rowRun, value);
            }
            else {
                runToRow.put(rowRun, 1L);
            }
            arraySize--;

        }

        for(int run : runs) {
            Assertions.assertEquals(20, runToRow.get(run), "batch "+ run +" contained other than 20 messages");
        }
    }
}
