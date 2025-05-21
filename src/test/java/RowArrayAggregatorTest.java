import com.teragrep.functions.dpf_02.RowArrayAggregator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
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
import java.util.ArrayList;

public class RowArrayAggregatorTest {

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
    public void testRowArrayAggregator() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        sparkSession.sparkContext().setLogLevel("ERROR");

        ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
        MemoryStream<Row> rowMemoryStream =
                new MemoryStream<>(1, sqlContext, encoder);

        //BatchCollect batchCollect = new BatchCollect("_time", 100, null);
        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        RowArrayAggregator aggregator = new RowArrayAggregator(testSchema);
        rowDataset = rowDataset.agg(aggregator.toColumn());
        StreamingQuery streamingQuery = startStream(rowDataset);

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
                streamingQuery.stop();
                Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
            }
        }

        Dataset<Row> ds = sqlContext.sql("SELECT * FROM AggTest");
        ds = ds.select(functions.explode(functions.col("`RowArrayAggregator(org.apache.spark.sql.Row)`.arrayOfInput")));
        ds = ds.select("col.*");
        ds.printSchema();
        ds.show(false);
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


    private StreamingQuery startStream(Dataset<Row> rowDataset) {
        return rowDataset
                .writeStream()
                .queryName("AggTest")
                .format("memory")
                .outputMode(OutputMode.Complete())
                .start();
    }
}
