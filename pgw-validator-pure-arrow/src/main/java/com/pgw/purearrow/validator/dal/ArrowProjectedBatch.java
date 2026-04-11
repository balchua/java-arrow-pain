package com.pgw.purearrow.validator.dal;

import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ArrowProjectedBatch {

    private final int rowCount;
    private final Map<String, FieldVector> vectors;

    private ArrowProjectedBatch(int rowCount, Map<String, FieldVector> vectors) {
        this.rowCount = rowCount;
        this.vectors = vectors;
    }

    static ArrowProjectedBatch create(VectorSchemaRoot root, List<String> columns) {
        Map<String, FieldVector> vectors = new LinkedHashMap<>();
        for (String column : columns) {
            vectors.put(column, root.getVector(column));
        }
        return new ArrowProjectedBatch(root.getRowCount(), vectors);
    }

    int rowCount() {
        return rowCount;
    }

    VarCharVector varchar(String field) {
        return (VarCharVector) vectors.get(field);
    }

    DecimalVector decimal(String field) {
        return (DecimalVector) vectors.get(field);
    }

    String getString(String field, int row) {
        return getString(varchar(field), row);
    }

    BigDecimal getDecimal(String field, int row) {
        return getDecimal(decimal(field), row);
    }

    private static String getString(VarCharVector vector, int row) {
        if (vector == null || vector.isNull(row)) {
            return null;
        }
        return vector.getObject(row).toString();
    }

    private static BigDecimal getDecimal(DecimalVector vector, int row) {
        if (vector == null || vector.isNull(row)) {
            return null;
        }
        return vector.getObject(row);
    }
}