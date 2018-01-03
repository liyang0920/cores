package cores.core;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.trevni.TrevniRuntimeException;

public class UnionInputBuffer extends BlockInputBuffer {
    private byte[] buf3;
    private int unionBits;
    private int count;
    private ValueType[] unionTypes;
    BlockInputBuffer buf;
    private int bitCount;
    int off;
    int xx;

    public UnionInputBuffer(ByteBuffer data, int count, int unionBits, ValueType[] unionTypes) {
        this.unionBits = unionBits;
        switch (unionBits) {
            case 1:
                xx = 0x01;
                break;
            case 2:
                xx = 0x03;
                break;
            case 4:
                xx = 0x0f;
                break;
            case 8:
                xx = 0xff;
        }
        this.count = count;
        this.unionTypes = unionTypes;
        long allBits = unionBits * count;
        int length = (int) allBits / 8;
        if (allBits % 8 > 0)
            length++;
        buf3 = new byte[length];
        data.get(buf3, 0, length);
        byte[] limit = new byte[data.limit() - length];
        data.get(limit);

        buf = new BlockInputBuffer(limit, count);
    }

    @Override
    public <T extends Comparable> T readValue(ValueType type) throws IOException {
        int i = buf3[off] >> bitCount;
        i &= xx;
        bitCount += unionBits;
        if (bitCount == 8) {
            bitCount = 0;
            off++;
        }
        T r;
        if (UnionOutputBuffer.isFixed(unionTypes[i]))
            r = readFixedValue(unionTypes[i]);
        else
            r = buf.readValue(unionTypes[i]);
        return r;
    }

    public <T extends Comparable> T readFixedValue(ValueType type) throws IOException {
        switch (type) {
            case NULL:
                buf.skipNull();
                return null;
            case BOOLEAN:
                return (T) Boolean.valueOf(readBoolean());
            case INT:
                return (T) Integer.valueOf(readFixed32());
            case LONG:
                return (T) Long.valueOf(readFixed64());
            case FIXED32:
                return (T) Integer.valueOf(readFixed32());
            case FIXED64:
                return (T) Long.valueOf(readFixed64());
            case FLOAT:
                return (T) Float.valueOf(readFloat());
            case DOUBLE:
                return (T) Double.valueOf(readDouble());
            default:
                throw new TrevniRuntimeException("this type is not fixed: " + type);
        }
    }

    @Override
    public void skipValue(ValueType type, int r) throws IOException {
        off += r * unionBits / 8;
        bitCount += (r * unionBits) % 8;
        if (bitCount >= 8) {
            off++;
            bitCount -= 8;
        }
        buf.skipBytes(r);
    }

    public boolean readBoolean() throws IOException {
        byte[] res = buf.readUnionFixed(1);
        return res[0] == (byte) 0 ? false : true;
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readFixed32());
    }

    public int readFixed32() throws IOException {
        byte[] res = buf.readUnionFixed(4);
        int n = (res[0] & 0xff) | ((res[1] & 0xff) << 8) | ((res[2] & 0xff) << 16) | ((res[3] & 0xff) << 24);
        return n;
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readFixed64());
    }

    public long readFixed64() throws IOException {
        return (readFixed32() & 0xFFFFFFFFL) | (((long) readFixed32()) << 32);
    }
}
