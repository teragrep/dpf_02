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
