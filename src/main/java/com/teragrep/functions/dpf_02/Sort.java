package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Sort {
    private final List<SortByClause> clauses;
    private final String defaultColumn;
    private final UserDefinedFunction ipStringToIntUDF = functions.udf(new ConvertIPStringToInt(), DataTypes.LongType);

    public Sort(final List<SortByClause> clauses) {
        this(clauses, "_time");
    }

    public Sort(final List<SortByClause> clauses, String defaultColumn) {
        this.clauses = List.copyOf(clauses);
        this.defaultColumn = defaultColumn;
    }

    public Dataset<Row> apply(final Dataset<Row> dataset) {
        final Dataset<Row> result;
        if (!clauses.isEmpty()) {
            final ArrayList<Object> columns = new ArrayList<>();

            for (final SortByClause clause : clauses) {
                final Column columnForClause = columnFor(dataset, clause);
                columns.add(columnForClause);
            }
            result = dataset.orderBy(columns.toArray(new Column[0]));
        } else if (hasDefaultColumn(dataset)) {
            result = dataset.orderBy(functions.col(defaultColumn).desc());
        } else {
            result = dataset;
        }
        return result;
    }

    private Column columnFor(final Dataset<Row> dataset, final SortByClause clause) {
        final SortByClause.Type type = clause.getSortAsType() == SortByClause.Type.AUTOMATIC
                ? detectedType(dataset, clause.getFieldName())
                : clause.getSortAsType();
        System.out.println("COLUMN TYPE: "+ type);
        final Column column = functions.col(clause.getFieldName());
        final Column expression;
        if (type.equals(SortByClause.Type.STRING)) {
            expression = column.cast(DataTypes.StringType);
        } else if (type.equals(SortByClause.Type.NUMERIC)) {
            expression = column.cast(DataTypes.DoubleType);
        } else if (type.equals(SortByClause.Type.IP_ADDRESS)) {
            expression = ipStringToIntUDF.apply(column);
        } else {
            expression = column;
        }

        final Column ordering;
        if (clause.isDescending()) {
            ordering = expression.desc();
        } else {
            ordering = expression.asc();
        }

        return ordering;
    }

    private SortByClause.Type detectedType(final Dataset<Row> dataset, final String fieldName) {
        final List<StructField> fields = Arrays.stream(dataset.schema().fields())
                .filter(candidate -> candidate.name().equals(fieldName))
                .collect(Collectors.toList());

        final boolean fieldExists = !fields.isEmpty();

        final DataType type = fieldExists
                ? fields.get(0).dataType()
                : DataTypes.NullType;

        final boolean isString = type.sameType(DataTypes.StringType);
        final boolean isNumeric = type.sameType(DataTypes.LongType)
                || type.sameType(DataTypes.IntegerType)
                || type.sameType(DataTypes.FloatType)
                || type.sameType(DataTypes.DoubleType)
                || type.sameType(DataTypes.TimestampType);

        final SortByClause.Type detectedType;

        if (isString) {
            detectedType = numericalStringType(dataset, fieldName);
        } else if (isNumeric) {
            detectedType = SortByClause.Type.NUMERIC;
        } else {
            detectedType = SortByClause.Type.DEFAULT;
        }
        return detectedType;
    }

    private SortByClause.Type numericalStringType(final Dataset<Row> dataset,final String fieldName) {
        final Column numerical = functions.col(fieldName).cast(DataTypes.DoubleType).isNotNull();
        final Dataset<Row> nonNumerical = dataset.filter(numerical.equalTo(false));

        return nonNumerical.isEmpty()
                ? SortByClause.Type.NUMERIC
                : SortByClause.Type.STRING;
    }

    private boolean hasDefaultColumn(final Dataset<Row> dataset) {
        final boolean result;
        if (defaultColumn.isEmpty()) {
            result = false;
        } else {
            boolean datasetContainsDefaultColumn = false;
            for (final String field : dataset.columns()) {
                if (field.equals(defaultColumn)) {
                    datasetContainsDefaultColumn = true;
                    break;
                }
            }
            result = datasetContainsDefaultColumn;
        }
        return result;
    }
}
