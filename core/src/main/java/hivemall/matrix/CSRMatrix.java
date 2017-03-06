/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.matrix;

import hivemall.utils.lang.Preconditions;

import java.util.Arrays;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Read-only CSR double Matrix.
 * 
 * @link http://netlib.org/linalg/html_templates/node91.html#SECTION00931100000000000000
 * @link http://www.cs.colostate.edu/~mcrob/toolbox/c++/sparseMatrix/sparse_matrix_compression.html
 */
public final class CSRMatrix extends AbstractMatrix {

    @Nonnull
    private final int[] rowPointers;
    @Nonnull
    private final int[] columnIndices;
    @Nonnull
    private final double[] values;

    @Nonnegative
    private final int numRows;
    @Nonnegative
    private final int numColumns;

    public CSRMatrix(@Nonnull int[] rowPointers, @Nonnull int[] columnIndices,
            @Nonnull double[] values, @Nonnegative int numColumns) {
        super();
        Preconditions.checkArgument(rowPointers.length >= 1,
            "rowPointers must be greather than 0: " + rowPointers.length);
        Preconditions.checkArgument(columnIndices.length == values.length, "#columnIndices ("
                + columnIndices.length + ") must be equals to #values (" + values.length + ")");
        this.rowPointers = rowPointers;
        this.columnIndices = columnIndices;
        this.values = values;
        this.numRows = rowPointers.length - 1;
        this.numColumns = numColumns;
    }

    @Override
    public boolean isSparse() {
        return true;
    }

    @Override
    public boolean readOnly() {
        return true;
    }

    @Override
    public boolean swappable() {
        return false;
    }

    @Override
    public int numRows() {
        return numRows;
    }

    @Override
    public int numColumns() {
        return numColumns;
    }

    @Override
    public int numColumns(@Nonnegative final int row) {
        checkRowIndex(row, numRows);

        int columns = rowPointers[row + 1] - rowPointers[row];
        return columns;
    }

    @Override
    public double[] getRow(@Nonnegative final int index) {
        final double[] row = new double[numColumns];
        eachNonZeroInRow(index, new VectorProcedure() {
            public void apply(int col, double value) {
                row[col] = value;
            }
        });
        return row;
    }

    @Override
    public double[] getRow(@Nonnegative final int index, @Nonnull final double[] dst) {
        Arrays.fill(dst, 0.d);
        eachNonZeroInRow(index, new VectorProcedure() {
            public void apply(int col, double value) {
                checkColIndex(col, numColumns);
                dst[col] = value;
            }
        });
        return dst;
    }

    @Override
    public double get(@Nonnegative final int row, @Nonnegative final int col,
            final double defaultValue) {
        checkIndex(row, col, numRows, numColumns);

        final int index = getIndex(row, col);
        if (index < 0) {
            return defaultValue;
        }
        return values[index];
    }

    @Override
    public double getAndSet(@Nonnegative final int row, @Nonnegative final int col,
            final double value) {
        checkIndex(row, col, numRows, numColumns);

        final int index = getIndex(row, col);
        if (index < 0) {
            throw new UnsupportedOperationException("Cannot update value in row " + row + ", col "
                    + col);
        }

        double old = values[index];
        values[index] = value;
        return old;
    }

    @Override
    public void set(@Nonnegative final int row, @Nonnegative final int col, final double value) {
        checkIndex(row, col, numRows, numColumns);

        final int index = getIndex(row, col);
        if (index < 0) {
            throw new UnsupportedOperationException("Cannot update value in row " + row + ", col "
                    + col);
        }
        values[index] = value;
    }

    private int getIndex(@Nonnegative final int row, @Nonnegative final int col) {
        int leftIn = rowPointers[row];
        int rightEx = rowPointers[row + 1];
        final int index = Arrays.binarySearch(columnIndices, leftIn, rightEx, col);
        if (index >= 0 && index >= values.length) {
            throw new IndexOutOfBoundsException("Value index " + index + " out of range "
                    + values.length);
        }
        return index;
    }

    @Override
    public void swap(int row1, int row2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void eachInRow(@Nonnegative int row, @Nonnull final VectorProcedure procedure) {
        checkRowIndex(row, numRows);

        final int endEx = rowPointers[row + 1];
        for (int col = 0, j = rowPointers[row]; col < numColumns; col++) {
            if (j < endEx && col == columnIndices[j]) {
                double v = values[j++];
                procedure.apply(col, v);
            } else {
                procedure.apply(col, 0.d);
            }
        }
    }

    @Override
    public void eachNonZeroInRow(@Nonnegative int row, @Nonnull final VectorProcedure procedure) {
        checkRowIndex(row, numRows);

        final int startIn = rowPointers[row];
        final int endEx = rowPointers[row + 1];
        for (int i = startIn; i < endEx; i++) {
            int col = columnIndices[i];
            double v = values[i];
            procedure.apply(col, v);
        }
    }

    @Override
    public MatrixBuilder builder() {
        return new CSRMatrixBuilder(1024, true);
    }

}