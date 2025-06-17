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
import com.teragrep.functions.dpf_02.operation.limit.LimitedList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class LimitedListTest {
    @Test
    void testLimit2() {
        final List<String> input = Arrays.asList("a", "b", "c", "d", "e");
        final LimitedList<String> limitedList = new LimitedList<>(2, input);
        Assertions.assertEquals(Arrays.asList("a", "b"), limitedList.toList());
    }

    @Test
    void testLimitMoreThanArraySize() {
        final List<String> input = Arrays.asList("a", "b", "c", "d", "e");
        final LimitedList<String> limitedList = new LimitedList<>(10, input);
        Assertions.assertEquals(input, limitedList.toList());
    }

    @Test
    void testLimit0() {
        final List<String> input = Arrays.asList("a", "b", "c", "d", "e");
        final LimitedList<String> limitedList = new LimitedList<>(0, input);
        Assertions.assertEquals(Collections.emptyList(), limitedList.toList());
    }

    @Test
    void testLimitNegative() {
        final List<String> input = Arrays.asList("a", "b", "c", "d", "e");
        final LimitedList<String> limitedList = new LimitedList<>(-1, input);
        final IllegalArgumentException iae = Assertions.assertThrows(IllegalArgumentException.class, limitedList::toList);
        Assertions.assertEquals("Limit cannot be a negative number", iae.getMessage());
    }

    @Test
    void testEquals() {
        EqualsVerifier.forClass(LimitedList.class).verify();
    }
}
