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
