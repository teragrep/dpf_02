import com.teragrep.functions.dpf_02.operation.sort.TimestampSort;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public final class TimestampSortTest {
    private static final StructType testSchema = new StructType(
            new StructField[]{
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build())
            }
    );

    final Row r0 = new GenericRowWithSchema(new Object[]{
            new Timestamp(1735689600L * 1000L),
    }, testSchema);

    final Row r1 = new GenericRowWithSchema(new Object[]{
            new Timestamp(1735689600L * 1000L + (3600L*1000L)),
    }, testSchema);

    final Row r2 = new GenericRowWithSchema(new Object[]{
            new Timestamp(1735689600L * 1000L + (3600L*2L*1000L)),
    }, testSchema);

    final Row r3 = new GenericRowWithSchema(new Object[]{
            new Timestamp(1735689600L * 1000L + (3600L*3L*1000L)),
    }, testSchema);

    final Row r4 = new GenericRowWithSchema(new Object[]{
            new Timestamp(1735689600L * 1000L + (3600L*4L*1000L)),
    }, testSchema);

    @Test
    void testSortDefault() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new TimestampSort().sort(rows);
        final List<Row> expected = Arrays.asList(r4,r3,r2,r1,r0);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testSortAscending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new TimestampSort(false).sort(rows);
        final List<Row> expected = Arrays.asList(r0,r1,r2,r3,r4);

        Assertions.assertEquals(expected, sorted);
    }
}
