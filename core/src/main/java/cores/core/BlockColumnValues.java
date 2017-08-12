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
public class BlockColumnValues<T extends Comparable> implements Iterator<T>, Iterable<T> {

    protected final ColumnDescriptor column;
    protected final ValueType type;
    protected final Codec codec;
    protected final Checksum checksum;
    protected final InputBuffer in;

    protected BlockInputBuffer values;
    protected int block = -1;
    protected int row = 0;
    protected T previous;
    protected int offset = 0;

    protected int arrayLength;
    //    protected long time;
    //    protected int readBlockSize;
    //    protected int seekedBlock;
    //    protected List<Long> blockTime;
    //    protected List<Long> blockStart;
    //    protected List<Long> blockEnd;
    //    protected List<Long> blockOffset;

    protected BlockColumnValues(ColumnDescriptor column) throws IOException {
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

    public String getName() {
        return column.metaData.getName();
    }

    public String getParentName() {
        if (column.metaData.getParent() != null)
            return column.metaData.getParent().getName();
        else
            return null;
    }

    public int getLayer() {
        return column.metaData.getLayer();
    }

    public boolean isArray() {
        return column.metaData.isArray();
    }

    public void create() throws IOException {
        offset = 0;
        seek(0);
    }

    //    public void createTime() {
    //        time = 0;
    //        blockTime = new ArrayList<Long>();
    //        blockStart = new ArrayList<Long>();
    //        blockEnd = new ArrayList<Long>();
    //        blockOffset = new ArrayList<Long>();
    //    }
    //
    //    public long getTime() {
    //        return time;
    //    }

    //    public List<Long> getBlockTime() {
    //        return blockTime;
    //    }
    //
    //    public List<Long> getBlockStart() {
    //        return blockStart;
    //    }
    //
    //    public List<Long> getBlockEnd() {
    //        return blockEnd;
    //    }
    //
    //    public List<Long> getBlockOffset() {
    //        return blockOffset;
    //    }

    //    public void createSeekBlock() {
    //        readBlockSize = 0;
    //        seekedBlock = 0;
    //    }
    //
    //    public int[] getSeekBlock() {
    //        return new int[] { readBlockSize, seekedBlock };
    //    }

    public int getBlockCount() {
        return column.blockCount();
    }

    /**
     * Seek to the named row.
     */
    public void seek(int r) throws IOException {
        if (r < row || r >= column.lastRow(block)) // not in current block
            startBlock(column.findBlock(r)); // seek to block start
        if (r > row) { // skip within block
            if (column.metaData.isArray())
                values.skipLength(r - row);
            else
                values.skipValue(type, r - row);
            row = r;
        }
        previous = null;
    }

    public void turnTo(int r) throws IOException {
        if (r < row || r >= column.lastRow(block)) // not in current block
            seekBlock(column.findBlock(r)); // seek to block start
        if (r > row) { // skip within block
            if (column.metaData.isArray())
                values.skipLength(r - row);
            else
                values.skipValue(type, r - row);
            row = r;
        }
        previous = null;
    }

    public void seekBlock(int block) throws IOException {
        for (int i = this.block + 1; i < block; i++) {
            in.seek(column.blockStarts[i]);
            int end = column.blocks[i].compressedSize;
            byte[] raw = new byte[end + checksum.size()];
            in.readFully(raw);
        }
        startBlock(block);
    }

    public void startBlock(int block) throws IOException {
        long s = System.nanoTime();
        //        readBlockSize++;
        //        seekedBlock += Math.abs(block - this.block - 1);
        //                if (skipLength != null) {
        //        if (this.block == -1)
        //            skipLength.add(column.blockStarts[block] - column.start);
        //        else
        //            skipLength.add(column.blockStarts[block] - column.blockStarts[this.block]
        //                    - column.blocks[this.block].compressedSize - checksum.size());
        //                }
        this.block = block;
        this.row = column.firstRows[block];

        in.seek(column.blockStarts[block]);
        int end = column.blocks[block].compressedSize;
        byte[] raw = new byte[end + checksum.size()];
        in.readFully(raw);
        ByteBuffer data = codec.decompress(ByteBuffer.wrap(raw, 0, end));
        if (!checksum.compute(data).equals(ByteBuffer.wrap(raw, end, checksum.size())))
            throw new IOException("Checksums mismatch.");
        values = new BlockInputBuffer(data, column.blocks[block].rowCount);
        long e = System.nanoTime();
        //        blockTime.add((e - s));
        //        blockStart.add(s);
        //        blockEnd.add(e);
        //        blockOffset.add(column.blockStarts[block]);
        //        time += e - s;
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
        if (column.metaData.isArray())
            throw new TrevniRuntimeException("Column is array: " + column.metaData.getName());
        try {
            startRow();
            return nextValue();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    public void readIO() throws IOException {
        for (int i = 0; i < column.blockCount(); i++) {
            startBlock(i);
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
    public int nextLength() {
        if (!column.metaData.isArray())
            throw new TrevniRuntimeException("Column is not array: " + column.metaData.getName());
        assert arrayLength == 0;
        try {
            offset = arrayLength = values.readLength();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
        return arrayLength;
    }

    /*
     * while the array column is incremently stored, return the array Length and the first offset.
     */
    public int[] nextLengthAndOffset() {
        if (!column.metaData.isArray())
            throw new TrevniRuntimeException("Column is not array: " + column.metaData.getName());
        assert arrayLength == 0;
        int[] res = new int[2];
        res[1] = offset;
        try {
            offset = values.readLength();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
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

    public void skipValue(int r) throws IOException {
        values.skipValue(type, r);
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
