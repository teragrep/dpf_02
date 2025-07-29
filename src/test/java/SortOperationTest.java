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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeoutException;

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
                    new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
                    new StructField("IP", DataTypes.StringType, false, new MetadataBuilder().build())
            }
    );

    @Test
    public void testTwoSortByClauses() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

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

        createData(batchCollect, sqlContext);

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

    @Test
    public void testTwoSortByClausesDescending() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ArrayList<SortByClause> sortByClauses = new ArrayList<>();
        SortByClause partSbc = new SortByClause();
        partSbc.setFieldName("partition");
        partSbc.setDescending(true);
        partSbc.setLimit(20);
        partSbc.setSortAsType(SortByClause.Type.AUTOMATIC);
        SortByClause offSbc = new SortByClause();
        offSbc.setFieldName("offset");
        offSbc.setDescending(true);
        offSbc.setLimit(20);
        offSbc.setSortAsType(SortByClause.Type.AUTOMATIC);

        sortByClauses.add(partSbc);
        sortByClauses.add(offSbc);

        BatchCollect batchCollect = new BatchCollect("_time", 20, sortByClauses);

        createData(batchCollect, sqlContext);

        List<Row> partition = batchCollect.getCollectedAsDataframe().select("partition").collectAsList();
        List<Row> offset = batchCollect.getCollectedAsDataframe().select("offset").collectAsList();

        // should be first sorted by partition and then in those partitions sorted by offset
        Assertions.assertEquals("[3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0]",
                Arrays.toString(partition.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals("[4, 3, 2, 1, 0, 4, 3, 2, 1, 0, 4, 3, 2, 1, 0, 4, 3, 2, 1, 0]",
                Arrays.toString(offset.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals(20, partition.size());
        Assertions.assertEquals(20, offset.size());
    }

    @Test
    public void testTwoSortByClauseDescending_IP() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ArrayList<SortByClause> sortByClauses = new ArrayList<>();
        SortByClause partSbc = new SortByClause();
        partSbc.setFieldName("partition");
        partSbc.setDescending(true);
        partSbc.setLimit(20);
        partSbc.setSortAsType(SortByClause.Type.NUMERIC);
        SortByClause ipSbc = new SortByClause();
        ipSbc.setFieldName("IP");
        ipSbc.setDescending(true);
        ipSbc.setLimit(20);
        ipSbc.setSortAsType(SortByClause.Type.IP_ADDRESS);

        sortByClauses.add(partSbc);
        sortByClauses.add(ipSbc);

        BatchCollect batchCollect = new BatchCollect("_time", 20, sortByClauses);

        createData(batchCollect, sqlContext);

        List<Row> partition = batchCollect.getCollectedAsDataframe().select("partition").collectAsList();
        List<Row> ip = batchCollect.getCollectedAsDataframe().select("ip").collectAsList();

        // should be first sorted by partition and then in those partitions sorted by IP
        Assertions.assertEquals("[3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0]",
                Arrays.toString(partition.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals("[127.0.0.4, 127.0.0.3, 127.0.0.2, 127.0.0.1, 127.0.0.0, 127.0.0.4, " +
                        "127.0.0.3, 127.0.0.2, 127.0.0.1, 127.0.0.0, 127.0.0.4, 127.0.0.3, 127.0.0.2, " +
                        "127.0.0.1, 127.0.0.0, 127.0.0.4, 127.0.0.3, 127.0.0.2, 127.0.0.1, 127.0.0.0]",
                Arrays.toString(ip.stream().map(r -> r.getAs(0).toString()).toArray()));

        Assertions.assertEquals(20, partition.size());
        Assertions.assertEquals(20, ip.size());
    }

    @Test
    public void testThreeSortByClauses() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ArrayList<SortByClause> sortByClauses = new ArrayList<>();
        SortByClause partSbc = new SortByClause();
        partSbc.setFieldName("partition");
        partSbc.setDescending(false);
        partSbc.setLimit(20);
        partSbc.setSortAsType(SortByClause.Type.NUMERIC);
        SortByClause hostSbc = new SortByClause();
        hostSbc.setFieldName("host");
        hostSbc.setDescending(true); // host_B, host_A
        hostSbc.setLimit(20);
        hostSbc.setSortAsType(SortByClause.Type.STRING);
        SortByClause offSbc = new SortByClause();
        offSbc.setFieldName("offset");
        offSbc.setDescending(false);
        offSbc.setLimit(20);
        offSbc.setSortAsType(SortByClause.Type.NUMERIC);

        sortByClauses.add(partSbc);
        sortByClauses.add(hostSbc);
        sortByClauses.add(offSbc);

        BatchCollect batchCollect = new BatchCollect("_time", 20, sortByClauses);

        createData(batchCollect, sqlContext);

        List<Row> partition = batchCollect.getCollectedAsDataframe().select("partition").collectAsList();
        List<Row> offset = batchCollect.getCollectedAsDataframe().select("offset").collectAsList();
        List<Row> host = batchCollect.getCollectedAsDataframe().select("host").collectAsList();

        // should be first sorted by partition and then in those partitions sorted by host and in host sorted by offset
        Assertions.assertEquals("[0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3]",
                Arrays.toString(partition.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals("[host_B, host_B, host_A, host_A, host_A, host_B, host_B, host_A, host_A, host_A, " +
                        "host_B, host_B, host_A, host_A, host_A, host_B, host_B, host_A, host_A, host_A]",
                Arrays.toString(host.stream().map(r -> r.getAs(0).toString()).toArray()));
        Assertions.assertEquals("[3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2]",
                Arrays.toString(offset.stream().map(r -> r.getAs(0).toString()).toArray()));

        Assertions.assertEquals(20, partition.size());
        Assertions.assertEquals(20, offset.size());
        Assertions.assertEquals(20, host.size());
    }

    private void createData(BatchCollect batchCollect, SQLContext sqlContext) {
        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        StreamingQuery streamingQuery = Assertions.assertDoesNotThrow(() -> startStream(rowDataset, batchCollect));

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
                            counter < 3 ? "host_A" : "host_B",
                            "input",
                            String.valueOf(run),
                            counter,
                            "127.0.0." + counter,
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
                Assertions.assertDoesNotThrow(streamingQuery::stop);
                Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
            }
        }
    }

    private Seq<Row> makeRows(Timestamp _time,
                              String _raw,
                              String index,
                              String sourcetype,
                              String host,
                              String source,
                              String partition,
                              Long offset,
                              String ip,
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
                offset,
                ip
        );

        while (amount > 0) {
            rowArrayList.add(row);
            amount--;
        }

        Seq<Row> rowSeq = JavaConverters.asScalaIteratorConverter(rowArrayList.iterator()).asScala().toSeq();

        return rowSeq;
    }


    private StreamingQuery startStream(Dataset<Row> rowDataset, BatchCollect batchCollect) throws TimeoutException {
        return rowDataset
                .writeStream()
                .foreachBatch(
                        new VoidFunction2<Dataset<Row>, Long>() {
                            @Override
                            public void call(Dataset<Row> batchDF, Long batchId) throws Exception {
                                batchCollect.collect(batchDF, batchId, Collections.emptyList(), false);
                            }
                        }
                )
                .outputMode("append")
                .start();
    }
}