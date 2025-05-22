import com.teragrep.functions.dpf_02.aggregate.RowArrayAggregator;
import com.teragrep.functions.dpf_02.aggregate.RowBuffer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

public final class EncoderTest {
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
    void testBufferEncoder() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        RowBuffer buf = new RowBuffer();
        Row input = RowFactory.create(
                new java.sql.Timestamp(ZonedDateTime.of(2010,1,1,0,0,0,0, ZoneId.of("UTC")).toInstant().toEpochMilli()),
                "_raw",
                "index",
                "sourcetype",
                "host",
                "source",
                "partition",
                "9999"
        );
        buf = buf.reduce(input);
        Dataset<RowBuffer> ds = sparkSession.createDataset(Collections.singletonList(buf), new RowArrayAggregator(testSchema).bufferEncoder());
        List<RowBuffer> collectedBuffers = ds.collectAsList();
        Assertions.assertEquals(1, collectedBuffers.size());
        RowBuffer collectedBuffer = collectedBuffers.get(0);
        List<Row> rowsFromCollectedBuffer = collectedBuffer.toList();
        Assertions.assertEquals(1, rowsFromCollectedBuffer.size());
        // Should be the same as input row
        Assertions.assertEquals(input, rowsFromCollectedBuffer.get(0));
    }
}
