package cores.avro;

import static cores.avro.AvroColumnator.isSimple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.trevni.Input;
import org.apache.trevni.TrevniRuntimeException;

import cores.core.BatchColumnFileReader;
import cores.core.BlockColumnValues;
import cores.core.FileColumnMetaData;
import cores.core.ValueType;

public class FilterBatchColumnReader<D> implements Closeable {
    BatchColumnFileReader reader;
    protected GenericData model;
    protected BlockColumnValues[] values;
    protected int[] readNO;
    protected int[] arrayWidths;
    protected int column;
    protected int[] arrayValues;
    protected HashMap<String, Integer> columnsByName;
    protected Schema readSchema;
    protected FilterOperator[] filters; //ensure that the filters is sorted from small to big layer

    protected String readParent;
    protected HashMap<String, Integer> readLength;
    protected Object[][] readValue;
    protected String currentParent;
    protected int currentLayer;
    protected BitSet filterSet;
    protected ArrayList<BitSet> chooseSet;
    protected HashMap<String, Integer> bitSetMap;
    protected HashMap<String, BitSet> filterSetMap;
    protected int[] readIndex; //the index of the next Record in every column;
    protected int all; //the number of the remain values in the disk;
    protected int[] setStart; //the start of the bitset of every layer
    protected int[] readSet; //the index of chooseSet of every read column

    //    protected long timeIO;
    //    protected int readBlockSize;
    //    protected int seekedBlock;
    //    protected int blockCount;
    //    protected List<Long> blockTime;
    //    protected List<Long> blockStart;
    //    protected List<Long> blockEnd;
    //    protected List<Long> blockOffset;

    static int max = 100000;

    public FilterBatchColumnReader() {

    }

    public FilterBatchColumnReader(File file) throws IOException {
        this(file, null, GenericData.get());
    }

    public FilterBatchColumnReader(Input data, Input head) throws IOException {
        this(data, head, null, GenericData.get());
    }

    public FilterBatchColumnReader(File file, FilterOperator[] filters) throws IOException {
        this(file, filters, GenericData.get());
    }

    public FilterBatchColumnReader(Input data, Input head, FilterOperator[] filters) throws IOException {
        this(data, head, filters, GenericData.get());
    }

    public FilterBatchColumnReader(File file, FilterOperator[] filters, GenericData model) throws IOException {
        this.reader = new BatchColumnFileReader(file);
        this.filters = filters;
        columnsByName = reader.getColumnsByName();
        this.model = model;
        this.values = new BlockColumnValues[reader.getColumnCount()];
        int le = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = reader.getValues(i);
            //            if (values[i].isArray()) {
            //                                arrayValues.put(values[i].getName(), i);
            //                le++;
            //            }
        }
    }

    public FilterBatchColumnReader(Input data, Input head, FilterOperator[] filters, GenericData model)
            throws IOException {
        this.reader = new BatchColumnFileReader(data, head);
        this.filters = filters;
        columnsByName = reader.getColumnsByName();
        this.model = model;
        this.values = new BlockColumnValues[reader.getColumnCount()];
        int le = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = reader.getValues(i);
        }
    }

    //    public long getTimeIO() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            timeIO += values[readNO[i]].getTime();
    //        }
    //        return timeIO;
    //    }
    //
    //    public List<Long> getBlockTime() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockTime.addAll(values[readNO[i]].getBlockTime());
    //        }
    //        return blockTime;
    //    }
    //
    //    public List<Long> getBlockStart() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockStart.addAll(values[readNO[i]].getBlockStart());
    //        }
    //        return blockStart;
    //    }
    //
    //    public List<Long> getBlockEnd() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockEnd.addAll(values[readNO[i]].getBlockEnd());
    //        }
    //        return blockEnd;
    //    }
    //
    //    public List<Long> getBlockOffset() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockOffset.addAll(values[readNO[i]].getBlockOffset());
    //        }
    //        return blockOffset;
    //    }
    //
    //    public int[] getFilterBlock() {
    //        return new int[] { readBlockSize, seekedBlock, blockCount };
    //    }
    //
    //    public int[] getBlockSeekRes() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            readBlockSize += values[readNO[i]].getSeekBlock()[0];
    //            seekedBlock += values[readNO[i]].getSeekBlock()[1];
    //            blockCount += values[readNO[i]].getBlockCount();
    //        }
    //        return new int[] { readBlockSize, seekedBlock, blockCount };
    //    }

    public void filter() throws IOException {
        assert (filters != null);
        //        timeIO = 0;
        //        blockTime = new ArrayList<Long>();
        //        blockStart = new ArrayList<Long>();
        //        blockEnd = new ArrayList<Long>();
        //        blockOffset = new ArrayList<Long>();
        //        readBlockSize = 0;
        //        seekedBlock = 0;
        //        blockCount = 0;
        String column = filters[0].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
        filterSet = new BitSet(values[tm].getLastRow());
        currentParent = values[tm].getParentName();
        currentLayer = values[tm].getLayer();
        int i = 0;
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        values[tm].create();
        while (values[tm].hasNext()) {
            if (filters[0].isMatch(values[tm].next())) {
                filterSet.set(i);
            }
            i++;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
        filterSetMap = new HashMap<String, BitSet>();
        if (currentParent != null)
            filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        for (int c = 1; c < filters.length - 1; c++) {
            filter(c);
            if (currentParent != null)
                filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        }
        if (filters.length > 1)
            filter(filters.length - 1);

        if (currentParent != null) {
            String parent = new String(currentParent);
            while (parent != null) {
                filterSetMap.remove(parent);
                parent = values[columnsByName.get(parent)].getParentName();
            }
        }
        chooseSet = new ArrayList<BitSet>();
        chooseSet.add((BitSet) filterSet.clone());
        bitSetMap = new HashMap<String, Integer>();
        bitSetMap.put(currentParent, 0);
    }

    private void filter(int c) throws IOException {
        String column = filters[c].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
        String parent = values[tm].getParentName();
        int layer = values[tm].getLayer();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
            filterSetTran(tm);
        }
        int m = filterSet.nextSetBit(0);
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        values[tm].create();
        while (m != -1) {
            values[tm].seek(m);
            if (!filters[c].isMatch(values[tm].next())) {
                filterSet.set(m, false);
            }
            if (++m > filterSet.length())
                break;
            m = filterSet.nextSetBit(m);
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
    }

    public void filterNoCasc() throws IOException {
        assert (filters != null);
        //        timeIO = 0;
        //        blockTime = new ArrayList<Long>();
        //        blockStart = new ArrayList<Long>();
        //        blockEnd = new ArrayList<Long>();
        //        blockOffset = new ArrayList<Long>();
        //        readBlockSize = 0;
        //        seekedBlock = 0;
        //        blockCount = 0;
        String column = filters[0].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
        filterSet = new BitSet(values[tm].getLastRow());
        currentParent = values[tm].getParentName();
        currentLayer = values[tm].getLayer();
        int i = 0;
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        values[tm].create();
        while (values[tm].hasNext()) {
            if (filters[0].isMatch(values[tm].next())) {
                filterSet.set(i);
            }
            i++;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
        filterSetMap = new HashMap<String, BitSet>();
        if (currentParent != null)
            filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        for (int c = 1; c < filters.length - 1; c++) {
            filterNoCasc(c);
            if (currentParent != null)
                filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        }
        if (filters.length > 1)
            filterNoCasc(filters.length - 1);

        if (currentParent != null) {
            String parent = new String(currentParent);
            while (parent != null) {
                filterSetMap.remove(parent);
                parent = values[columnsByName.get(parent)].getParentName();
            }
        }
        chooseSet = new ArrayList<BitSet>();
        chooseSet.add((BitSet) filterSet.clone());
        bitSetMap = new HashMap<String, Integer>();
        bitSetMap.put(currentParent, 0);
    }

    public void filterReadIO() throws IOException {
        assert (filters != null);
        String column = filters[0].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
        currentParent = values[tm].getParentName();
        currentLayer = values[tm].getLayer();
        values[tm].readIO();
        values[tm].create();
        for (int c = 1; c < filters.length; c++) {
            column = filters[c].getName();
            tm = columnsByName.get(column);
            if (tm == null)
                throw new TrevniRuntimeException("No filter column named: " + column);
            String parent = values[tm].getParentName();
            int layer = values[tm].getLayer();
            if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
                List<String> left = new ArrayList<String>();
                List<String> right = new ArrayList<String>();
                if (currentLayer > layer) {
                    for (int i = currentLayer; i > layer; i--) {
                        left.add(currentParent);
                        int col = columnsByName.get(currentParent);
                        currentParent = values[col].getParentName();
                    }
                    currentLayer = layer;
                }
                if (layer > currentLayer) {
                    for (int i = layer; i > currentLayer; i--) {
                        right.add(parent);
                        int col = columnsByName.get(parent);
                        parent = values[col].getParentName();
                    }
                    layer = currentLayer;
                }
                while (currentParent != null && !currentParent.equals(parent)) {
                    left.add(currentParent);
                    int l = columnsByName.get(currentParent);
                    currentParent = values[l].getParentName();
                    right.add(parent);
                    int r = columnsByName.get(parent);
                    parent = values[r].getParentName();
                }

                for (int i = 0; i < left.size(); i++) {
                    String array = left.get(i);
                    int col = columnsByName.get(array);
                    values[col].readIO();
                    values[col].create();
                }

                for (int i = right.size() - 1; i >= 0; i--) {
                    String array = right.get(i);
                    int col = columnsByName.get(array);
                    values[col].readIO();
                    values[col].create();
                }
                currentLayer = values[tm].getLayer();
                currentParent = values[tm].getParentName();
            }
        }
    }

    private void filterNoCasc(int c) throws IOException {
        String column = filters[c].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
        String parent = values[tm].getParentName();
        int layer = values[tm].getLayer();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
            filterSetTran(tm);
        }
        BitSet set = new BitSet(values[tm].getLastRow());
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        values[tm].create();
        int n = 0;
        while (values[tm].hasNext()) {
            if (filters[c].isMatch(values[tm].next()))
                set.set(n);
            ++n;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
        set.and(filterSet);
        filterSet = set;
    }

    private void filterSetTran(int c) throws IOException {
        List<String> left = new ArrayList<String>();
        List<String> right = new ArrayList<String>();
        int layer = values[c].getLayer();
        String parent = values[c].getParentName();
        if (currentLayer > layer) {
            for (int i = currentLayer; i > layer; i--) {
                left.add(currentParent);
                int col = columnsByName.get(currentParent);
                currentParent = values[col].getParentName();
            }
            currentLayer = layer;
        }
        if (layer > currentLayer) {
            for (int i = layer; i > currentLayer; i--) {
                right.add(parent);
                int col = columnsByName.get(parent);
                parent = values[col].getParentName();
            }
            layer = currentLayer;
        }
        while (currentParent != null && !currentParent.equals(parent)) {
            left.add(currentParent);
            int l = columnsByName.get(currentParent);
            currentParent = values[l].getParentName();
            right.add(parent);
            int r = columnsByName.get(parent);
            parent = values[r].getParentName();
        }

        for (int i = 0; i < left.size(); i++) {
            String array = left.get(i);
            upTran(array);
            String arr = values[columnsByName.get(array)].getParentName();
            if (arr != null)
                filterSetMap.put(arr, (BitSet) filterSet.clone());
        }

        for (int i = right.size() - 1; i >= 0; i--) {
            String array = right.get(i);
            downTran(array);
            BitSet f = filterSetMap.get(array);
            if (f != null) {
                filterSet.and(f);
            }
        }
        currentLayer = values[c].getLayer();
        currentParent = values[c].getParentName();
    }

    private void upTran(String array) throws IOException {
        int col = columnsByName.get(array);
        BitSet set = new BitSet(values[col].getLastRow());
        int m = filterSet.nextSetBit(0);
        int n = 0;
        //        values[col].createTime();
        //        values[col].createSeekBlock();
        values[col].create();
        while (m != -1 && values[col].hasNext()) {
            values[col].startRow();
            int max = values[col].nextLength();
            if (max > m) {
                set.set(n);
                if (++m > filterSet.length())
                    m = -1;
                else {
                    m = filterSet.nextSetBit(m);
                    while (m != -1 && m < max)
                        m = filterSet.nextSetBit(++m);
                }
            }
            n++;
        }
        filterSet = set;
        //        timeIO += values[col].getTime();
        //        blockTime.addAll(values[col].getBlockTime());
        //        blockStart.addAll(values[col].getBlockStart());
        //        blockEnd.addAll(values[col].getBlockEnd());
        //        blockOffset.addAll(values[col].getBlockOffset());
        //        readBlockSize += values[col].getSeekBlock()[0];
        //        seekedBlock += values[col].getSeekBlock()[1];
        //        blockCount += values[col].getBlockCount();
    }

    private void downTran(String array) throws IOException {
        int col = columnsByName.get(array);
        BitSet set = new BitSet(values[col + 1].getLastRow());
        int p = filterSet.nextSetBit(0);
        int q = -1;
        //        values[col].createTime();
        //        values[col].createSeekBlock();
        values[col].create();
        if (p == 0) {
            values[col].startRow();
            int[] res = values[col].nextLengthAndOffset();
            if (res[0] > 0)
                set.set(res[1], res[0]);
            q = p;
            p = filterSet.nextSetBit(1);
        }
        while (p != -1) {
            if (p == q + 1) {
                values[col].startRow();
                int[] res = values[col].nextLengthAndOffset();
                if (res[0] > 0)
                    set.set(res[1], res[0] + res[1]);
                //                for (int j = 0; j < res[0]; j++)
                //                    set.set(j + res[1]);
            } else {
                values[col].seek(p - 1);
                values[col].startRow();
                values[col].nextLengthAndOffset();
                values[col].startRow();
                int[] res = values[col].nextLengthAndOffset();
                if (res[0] > 0)
                    set.set(res[1], res[0] + res[1]);
                //                for (int j = 0; j < res[0]; j++)
                //                    set.set(j + res[1]);
            }
            q = p;
            if (++p > filterSet.length())
                break;
            p = filterSet.nextSetBit(p);
        }
        filterSet = set;
        //        timeIO += values[col].getTime();
        //        blockTime.addAll(values[col].getBlockTime());
        //        blockStart.addAll(values[col].getBlockStart());
        //        blockEnd.addAll(values[col].getBlockEnd());
        //        blockOffset.addAll(values[col].getBlockOffset());
        //        readBlockSize += values[col].getSeekBlock()[0];
        //        seekedBlock += values[col].getSeekBlock()[1];
        //        blockCount += values[col].getBlockCount();
    }

    public int getColumnNO(String name) {
        Integer tm = columnsByName.get(name);
        if (tm == null)
            throw new TrevniRuntimeException("No column named: " + name);
        return tm;
    }

    public ValueType getType(int columnNo) {
        return values[columnNo].getType();
    }

    public ValueType[] getTypes() {
        ValueType[] res = new ValueType[values.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = values[i].getType();
        }
        return res;
    }

    public void createSchema(Schema s) {
        readSchema = s;
        AvroColumnator readColumnator = new AvroColumnator(s);
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        arrayWidths = readColumnator.getArrayWidths();
        readNO = new int[readColumns.length];
        //        int le = 0;
        for (int i = 0; i < readColumns.length; i++) {
            readNO[i] = reader.getColumnNumber(readColumns[i].getName());
            //            if (values[readNO[i]].getType() == ValueType.NULL) {
            //                le++;
            //            }
            //            values[readNO[i]].createLength();
        }
        readParent = values[readNO[0]].getParentName();
        //        if (le > 0) {
        //            arrayValues = new int[le];
        //        }
    }

    private void readSetTran(int c) throws IOException {
        List<String> left = new ArrayList<String>();
        List<String> right = new ArrayList<String>();
        int layer = values[c].getLayer();
        String parent = values[c].getParentName();
        if (currentLayer > layer) {
            for (int i = currentLayer; i > layer; i--) {
                left.add(currentParent);
                int col = columnsByName.get(currentParent);
                currentParent = values[col].getParentName();
            }
            currentLayer = layer;
        }
        if (layer > currentLayer) {
            for (int i = layer; i > currentLayer; i--) {
                right.add(parent);
                int col = columnsByName.get(parent);
                parent = values[col].getParentName();
            }
            layer = currentLayer;
        }
        while (currentParent != null && !currentParent.equals(parent)) {
            left.add(currentParent);
            int l = columnsByName.get(currentParent);
            currentParent = values[l].getParentName();
            right.add(parent);
            int r = columnsByName.get(parent);
            parent = values[r].getParentName();
        }

        for (int i = 0; i < left.size(); i++) {
            String array = left.get(i);
            upTran(array);
            String arr = values[columnsByName.get(array)].getParentName();
            bitSetMap.put(arr, chooseSet.size());
            chooseSet.add(filterSet);
        }

        for (int i = right.size() - 1; i >= 0; i--) {
            String array = right.get(i);
            downTran(array);
            bitSetMap.put(array, chooseSet.size());
            BitSet f = filterSetMap.get(array);
            if (f != null) {
                filterSet.and(f);
            }
            chooseSet.add(filterSet);
        }
        currentLayer = values[c].getLayer();
        currentParent = values[c].getParentName();
    }

    public void createFilterRead() throws IOException {
        createFilterRead(max);
    }

    public void createFilterRead(int max) throws IOException {
        this.max = max;
        for (int i = 0; i < readNO.length; i++) {
            //            values[readNO[i]].createTime();
            //            values[readNO[i]].createSeekBlock();
            values[readNO[i]].create();
        }
        readValue = new Object[readNO.length][];
        readLength = new HashMap<String, Integer>();
        readImplPri();
    }

    private void readImplPri() throws IOException {
        long start = System.currentTimeMillis();
        setStart = new int[readNO.length];
        readSet = new int[readNO.length];
        int layer = values[readNO[0]].getLayer();
        String parent = values[readNO[0]].getParentName();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent)))
            readSetTran(readNO[0]);
        all = filterSet.cardinality();
        if (all > max) {
            readLength.put(readParent, max);
        } else {
            readLength.put(readParent, all);
        }
        for (int i = 0; i < readNO.length; i++) {
            parent = values[readNO[i]].getParentName();
            Integer set = bitSetMap.get(parent);
            if (set == null) {
                readSetTran(readNO[i]);
                readSet[i] = chooseSet.size() - 1;
            } else {
                readSet[i] = set;
                filterSet = chooseSet.get(set);
                currentParent = parent;
                currentLayer = values[readNO[i]].getLayer();
            }
            setStart[i] = filterSet.nextSetBit(0);
        }
        filterSetMap.clear();
        filterSetMap = null;
        long end = System.currentTimeMillis();
        System.out.println("read set tran time: " + (end - start));

        for (int i = 0; i < readNO.length; i++) {
            filterSet = chooseSet.get(readSet[i]);
            currentParent = values[readNO[i]].getParentName();
            currentLayer = values[readNO[i]].getLayer();
            readPri(i);
        }
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    private void readImpl() throws IOException {
        if (all == 0)
            return;
        if (all > max) {
            readLength.put(readParent, max);
        } else {
            readLength.put(readParent, all);
        }
        for (int i = 0; i < readNO.length; i++) {
            filterSet = chooseSet.get(readSet[i]);
            currentParent = values[readNO[i]].getParentName();
            currentLayer = values[readNO[i]].getLayer();
            readPri(i);
        }
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    private void readPri(int c) throws IOException {
        int length = readLength.get(currentParent);
        readValue[c] = new Object[length];
        if (values[readNO[c]].isArray()) {
            BitSet set = chooseSet.get(readSet[c + 1]);
            int changeArr = 0;
            int in = 0;
            int p = setStart[c];
            //            int q = -1;
            int m = setStart[c + 1];
            //            if (p == 0) {
            //                int[] res = values[readNO[c]].nextLengthAndOffset();
            //                changeArr += res[0];
            //                readValue[c][in] = res[0];
            //                in++;
            //                q = p;
            //                p = filterSet.nextSetBit(1);
            //            }
            while (in < length) {
                //                int[] res;
                //                values[readNO[c]].seek(p - 1);
                //                values[readNO[c]].nextLengthAndOffset();
                //                res = values[readNO[c]].nextLengthAndOffset();
                //                changeArr += res[0];
                //                readValue[c][in] = res[0];
                values[readNO[c]].seek(p);
                values[readNO[c]].startRow();
                int res = values[readNO[c]].nextLength();
                int re = 0;
                while (m != -1 && res > m) {
                    ++re;
                    m = set.nextSetBit(++m);
                }
                changeArr += re;
                readValue[c][in] = re;
                in++;
                p = filterSet.nextSetBit(++p);
            }
            readLength.put(values[readNO[c]].getName(), changeArr);
            setStart[c] = p;
        } else {
            int in = 0;
            int m = setStart[c];
            while (in < length) {
                values[readNO[c]].seek(m);
                readValue[c][in] = values[readNO[c]].next();
                in++;
                m = filterSet.nextSetBit(++m);
            }
            setStart[c] = m;
        }
    }

    public void setFilters(FilterOperator[] filters) {
        this.filters = filters;
    }

    public D next() {
        try {
            if (readIndex[0] == readValue[0].length) {
                readImpl();
            }
            column = 0;
            return (D) read(readSchema);
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    public boolean hasNext() {
        return readIndex[0] < readValue[0].length || all > 0;
    }

    public Object read(Schema s) throws IOException {
        if (isSimple(s)) {
            return readValue(s, column++);
        }
        final int startColumn = column;

        switch (s.getType()) {
            case RECORD:
                Object record = model.newRecord(null, s);
                for (Field f : s.getFields()) {
                    Object value = read(f.schema());
                    model.setField(record, f.name(), f.pos(), value);
                }
                return record;
            case ARRAY:
                int length = (int) readValue[column][readIndex[column]++];

                //                values[readNO[column]].startRow();
                //                int[] rr = values[readNO[column]].nextLengthAndOffset();
                //                length = rr[0];
                List elements = (List) new GenericData.Array(length, s);
                for (int i = 0; i < length; i++) {
                    this.column = startColumn;
                    Object value;
                    if (isSimple(s.getElementType()))
                        value = readValue(s, readNO[++column]);
                    else {
                        column++;
                        value = read(s.getElementType());
                    }
                    elements.add(value);
                }
                column = startColumn + arrayWidths[startColumn];
                return elements;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    public Object readValue(Schema s, int column) throws IOException {
        Object v = readValue[column][readIndex[column]++];

        switch (s.getType()) {
            case ENUM:
                return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
            case FIXED:
                return model.createFixed(null, ((ByteBuffer) v).array(), s);
        }

        return v;
    }

    public void create() throws IOException {
        for (BlockColumnValues v : values) {
            v.create();
        }
    }

    public void create(int no) throws IOException {
        values[no].create();
    }

    public int getRowCount(int columnNo) {
        return values[readNO[columnNo]].getLastRow();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
