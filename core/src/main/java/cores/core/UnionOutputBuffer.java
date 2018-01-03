package cores.core;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public class UnionOutputBuffer extends BlockOutputBuffer {
    private byte[] buf3;
    private int union;
    private ValueType[] unionTypes;
    private int count3;
    private int bitCount3;
    private int unionBits;

    public UnionOutputBuffer(ValueType[] types, int unionBits) {
        super();
        this.union = types.length;
        buf3 = new byte[COUNT];
        unionTypes = types;
        bitCount3 = 0;
        this.unionBits = unionBits;
    }

    public boolean isFull() {
        return (count1 + count2 + count3) >= BLOCK_SIZE;
    }

    public int size() {
        return count1 + count2 + count3;
    }

    public void close() {
        super.close();
        buf3 = null;
        bitCount = 0;
        count3 = 0;
    }

    public void writeValue(Object value, int index) throws IOException {
        ValueType type = unionTypes[index];
        writeUnion(index);
        if (type.equals(ValueType.BOOLEAN))
            super.writeBooleanByte((Boolean) value);
        else
            super.writeValue(value, type);
        if (isFixed(type))
            writeNull();
    }

    public static boolean isFixed(ValueType type) {
        switch (type) {
            case NULL:
            case BOOLEAN:
            case INT:
            case LONG:
            case FIXED32:
            case FIXED64:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    private void writeUnion(int i) {
        if (bitCount3 == 0) {
            ensureUnion(1);
            count3++;
        }
        buf3[count3 - 1] |= ((byte) (i & 0xff)) << bitCount3;
        bitCount3 += unionBits;
        if (bitCount3 == 8)
            bitCount3 = 0;
    }

    private void ensureUnion(int n) {
        if (count3 + n > buf3.length)
            buf3 = Arrays.copyOf(buf3, Math.max(buf3.length << 1, count3 + n));
    }

    public synchronized void writeTo(OutputStream out) throws IOException {
        out.write(buf3, 0, count3);
        out.write(buf1, 0, count1);
        out.write(buf2, 0, count2);
    }

    public synchronized void reset() {
        count3 = 0;
        count1 = 0;
        count2 = 0;
        bitCount3 = 0;
    }
}
