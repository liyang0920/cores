/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cores.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.trevni.TrevniRuntimeException;

/**
 * An iterator over column values.
 */
public class ColumnValues<T extends Comparable> implements Iterator<T>, Iterable<T> {

    protected final ColumnDescriptor column;
    protected final ValueType type;
    protected final Codec codec;
    protected final Checksum checksum;
    protected final InputBuffer in;

    protected InputBuffer values;
    protected int block = -1;
    protected int row = 0;
    protected T previous;
    protected int offset = 0;

    protected int arrayLength;

    protected ColumnValues(ColumnDescriptor column) throws IOException {
        this.column = column;
        this.type = column.metaData.getType();
        this.codec = Codec.get(column.metaData);
        this.checksum = Checksum.get(column.metaData);
        this.in = new InputBuffer(column.dataFile);

        column.ensureBlocksRead();
    }

    /**
     * Return the current row number within this file.
     */
    public int getRow() {
        return row;
    }

    public int getLastRow() {
        return column.lastRow();
    }

    public ValueType getType() {
        return type;
    }

    public void create() throws IOException {
        offset = 0;
        seek(0);
    }

    public boolean isArray() {
        return column.metaData.isArray();
    }

    /**
     * Seek to the named row.
     */
    public void seek(int r) throws IOException {
        if (r < row || r >= column.lastRow(block)) // not in current block
            startBlock(column.findBlock(r)); // seek to block start
        while (r > row && hasNext()) { // skip within block
            values.skipValue(type);
            row++;
        }
        previous = null;
    }

    /**
     * Seek to the named value.
     */
    public void seek(T v) throws IOException {
        if (!column.metaData.hasIndexValues())
            throw new TrevniRuntimeException("Column does not have value index: " + column.metaData.getName());

        if (previous == null // not in current block?
                || previous.compareTo(v) > 0
                || (block != column.blockCount() - 1 && column.firstValues[block + 1].compareTo(v) <= 0))
            startBlock(column.findBlock(v)); // seek to block start

        while (hasNext()) { // scan block
            long savedPosition = values.tell();
            T savedPrevious = previous;
            if (next().compareTo(v) >= 0) {
                values.seek(savedPosition);
                previous = savedPrevious;
                row--;
                return;
            }
        }
    }

    public void startBlock(int block) throws IOException {
        this.block = block;
        this.row = column.firstRows[block];

        in.seek(column.blockStarts[block]);
        int end = column.blocks[block].compressedSize;
        byte[] raw = new byte[end + checksum.size()];
        in.readFully(raw);
        ByteBuffer data = codec.decompress(ByteBuffer.wrap(raw, 0, end));
        if (!checksum.compute(data).equals(ByteBuffer.wrap(raw, end, checksum.size())))
            throw new IOException("Checksums mismatch.");
        values = new InputBuffer(new InputBytes(data));
    }

    @Override
    public Iterator iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return block < column.blockCount() - 1 || row < column.lastRow(block);
    }

    @Override
    public T next() {
        if (column.metaData.isArray() || column.metaData.getParent() != null)
            throw new TrevniRuntimeException("Column is array: " + column.metaData.getName());
        try {
            startRow();
            return nextValue();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    /**
     * Expert: Must be called before any calls to {@link #nextLength()} or
     * {@link #nextValue()}.
     */
    public void startRow() throws IOException {
        if (row >= column.lastRow(block)) {
            if (block >= column.blockCount())
                throw new TrevniRuntimeException("Read past end of column.");
            startBlock(block + 1);
        }
        row++;
    }

    /**
     * Expert: Returns the next length in an array column.
     */
    public int nextLength() throws IOException {
        if (!column.metaData.isArray())
            throw new TrevniRuntimeException("Column is not array: " + column.metaData.getName());
        assert arrayLength == 0;
        offset = arrayLength = values.readLength();
        return arrayLength;
    }

    /*
     * while the array column is incremently stored, return the array Length and the first offset.
     */
    public int[] nextLengthAndOffset() throws IOException {
        if (!column.metaData.isArray())
            throw new TrevniRuntimeException("Column is not array: " + column.metaData.getName());
        assert arrayLength == 0;
        int[] res = new int[2];
        res[1] = offset;
        offset = values.readLength();
        res[0] = arrayLength = offset - res[1];
        return res;
    }

    /**
     * Expert: Returns the next value in a column.
     */
    public T nextValue() throws IOException {
        arrayLength--;
        return previous = values.<T> readValue(type);
    }

    public void skipValue() throws IOException {
        values.skipValue(type);
    }

    public int nextKey() throws IOException {
        arrayLength--;
        return values.readFixed32();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
