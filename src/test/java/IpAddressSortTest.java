import com.teragrep.functions.dpf_02.operation.sort.IpAddressSort;
import com.teragrep.functions.dpf_02.operation.sort.StringSort;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public final class IpAddressSortTest {
    private static final StructType testSchema = new StructType(
            new StructField[]{
                    new StructField("ip_address", DataTypes.StringType, false, new MetadataBuilder().build())
            }
    );

    final Row r0 = new GenericRowWithSchema(new Object[]{
            "127.0.0.1",
    }, testSchema);

    final Row r1 = new GenericRowWithSchema(new Object[]{
            "127.0.0.2",
    }, testSchema);

    final Row r2 = new GenericRowWithSchema(new Object[]{
            "127.0.0.3",
    }, testSchema);

    final Row r3 = new GenericRowWithSchema(new Object[]{
            "127.0.0.4",
    }, testSchema);

    final Row r4 = new GenericRowWithSchema(new Object[]{
            "127.0.0.5",
    }, testSchema);

    @Test
    void testSortDescending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new IpAddressSort("ip_address", true).sort(rows);
        final List<Row> expected = Arrays.asList(r4,r3,r2,r1,r0);

        Assertions.assertEquals(expected, sorted);
    }

    @Test
    void testSortAscending() {
        final List<Row> rows = Arrays.asList(r2,r4,r1,r0,r3);
        final List<Row> sorted = new IpAddressSort("ip_address", false).sort(rows);
        final List<Row> expected = Arrays.asList(r0,r1,r2,r3,r4);

        Assertions.assertEquals(expected, sorted);
    }
}
