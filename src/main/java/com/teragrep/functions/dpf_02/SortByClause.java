package com.teragrep.functions.dpf_02;

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

/**
 * Class for the different sortByClauses in the 'sort' command
 */
public class SortByClause implements Serializable {
    private boolean descending = false; // + or -
    private String fieldName = null; // sort based on field

    private int limit = 10000; // limit final amount of rows

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public enum Type { // type of sorting, e.g. auto(), str(), num(), ip() or just field name (default)
        DEFAULT, AUTOMATIC, STRING, NUMERIC, IP_ADDRESS
    }

    private Type sortAsType = Type.DEFAULT; // default is no special sorting

    public boolean isDescending() {
        return descending;
    }

    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Type getSortAsType() {
        return sortAsType;
    }

    public void setSortAsType(Type sortAsType) {
        this.sortAsType = sortAsType;
    }

    // Builds a string with a similar representation as in the original command
    @Override
    public String toString() {
        String rv = "";
        if (this.descending) {
            rv += "-";
        }
        else {
            rv += "+";
        }

        if (this.sortAsType.equals(Type.AUTOMATIC)) {
            rv += "auto(" + this.fieldName + ")";
        }
        else if (this.sortAsType.equals(Type.STRING)) {
            rv += "str(" + this.fieldName + ")";
        }
        else if (this.sortAsType.equals(Type.DEFAULT)) {
            rv += this.fieldName;
        }
        else if (this.sortAsType.equals(Type.IP_ADDRESS)) {
            rv += "ip(" + this.fieldName + ")";
        }
        else if (this.sortAsType.equals(Type.NUMERIC)) {
            rv += "num(" + this.fieldName + ")";
        }

        return rv;
    }
}
