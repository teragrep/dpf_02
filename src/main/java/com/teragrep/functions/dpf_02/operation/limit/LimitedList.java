package com.teragrep.functions.dpf_02.operation.limit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class LimitedList<T> implements Serializable {
    private final int limit;
    private final List<T> origin;

    public LimitedList(final int limit, final List<T> origin) {
        this.limit = limit;
        this.origin = origin;
    }

    private void validate() {
        if (limit < 0) {
            throw new IllegalArgumentException("Limit cannot be a negative number");
        }
    }

    public List<T> toList() {
        validate();
        List<T> newLst = new ArrayList<>();
        final int endIndex = Math.min(origin.size(), limit);
        for (int i = 0; i < endIndex; i++) {
            newLst.add(origin.get(i));
        }
        return newLst;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LimitedList<?> that = (LimitedList<?>) o;
        return limit == that.limit && Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, origin);
    }
}
