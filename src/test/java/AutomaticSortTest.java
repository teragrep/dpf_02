import com.teragrep.functions.dpf_02.operation.sort.AutomaticSort;
import com.teragrep.functions.dpf_02.operation.sort.StringSort;
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

public final class AutomaticSortTest {
    private static final StructType testSchema = new StructType(
            new StructField[]{
                    new StructField("numericString", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("string", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("double", DataTypes.DoubleType, false, new MetadataBuilder().build()),
                    new StructField("time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            }
    );

    final Row r0 = new GenericRowWithSchema(new Object[]{
            "0", "a", 1.0d, new Timestamp(0L)
    }, testSchema);

    final Row r1 = new GenericRowWithSchema(new Object[]{
            "1", "b", 1.1d, new Timestamp(1000L)
    }, testSchema);

    final Row r2 = new GenericRowWithSchema(new Object[]{
            "2", "c", 1.2d, new Timestamp(2000L)
    }, testSchema);

    final Row r3 = new GenericRowWithSchema(new Object[]{
            "3", "d", 1.3d, new Timestamp(3000L)
    }, testSchema);

    final Row r4 = new GenericRowWithSchema(new Object[]{
            "10", "e", 1.4d, new Timestamp(4000L)
    }, testSchema);

    @Test
    void testNumericStringDescending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("numericString", true).sort(rows);
        final List<Row> expected = Arrays.asList(r4,r3,r2,r1,r0);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testNumericStringAscending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("numericString", false).sort(rows);
        final List<Row> expected = Arrays.asList(r0,r1,r2,r3,r4);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testStringDescending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("string", true).sort(rows);
        final List<Row> expected = Arrays.asList(r4,r3,r2,r1,r0);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testStringAscending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("string", false).sort(rows);
        final List<Row> expected = Arrays.asList(r0,r1,r2,r3,r4);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testDoubleDescending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("double", true).sort(rows);
        final List<Row> expected = Arrays.asList(r4,r3,r2,r1,r0);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testDoubleAscending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("double", false).sort(rows);
        final List<Row> expected = Arrays.asList(r0,r1,r2,r3,r4);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testTimeDescending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("time", true).sort(rows);
        final List<Row> expected = Arrays.asList(r4,r3,r2,r1,r0);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testTimeAscending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new AutomaticSort("time", false).sort(rows);
        final List<Row> expected = Arrays.asList(r0,r1,r2,r3,r4);

        Assertions.assertEquals(expected, sorted);
    }
}
