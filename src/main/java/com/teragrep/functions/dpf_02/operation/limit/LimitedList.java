package com.teragrep.functions.dpf_02.operation.limit;
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
