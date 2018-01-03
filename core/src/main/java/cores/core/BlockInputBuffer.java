package cores.core;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.trevni.TrevniRuntimeException;

public class BlockInputBuffer {
    private int pos;
    private int offset;
    private int count;
    private byte[] buf;
    private int bitCount;

    protected int runLength; // length of run
    protected int runValue; // value of run

    private static final CharsetDecoder UTF8 = Charset.forName("UTF-8").newDecoder();

    public BlockInputBuffer() {

    }

    public BlockInputBuffer(ByteBuffer data, int count) {
        buf = data.array();
        this.count = count * 2;
        //        offset = count * 2;
    }

    public BlockInputBuffer(byte[] data, int count) {
        buf = data;
        this.count = count * 2;
        //        offset = count * 2;
    }

    public <T extends Comparable> T readValue(ValueType type) throws IOException {
        switch (type) {
            case NULL:
                return (T) null;
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
            case STRING:
                return (T) readString();
            case BYTES:
                return (T) readBytes(null);
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    public void skipValue(ValueType type, int r) throws IOException {
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                skipBoolean(r);
                break;
            case INT:
            case FIXED32:
            case FLOAT:
                skip(4 * r);
                break;
            case LONG:
            case FIXED64:
            case DOUBLE:
                skip(8 * r);
                break;
            case STRING:
            case BYTES:
                skipBytes(r);
                break;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    public void skip(long length) throws IOException {
        pos += length;
    }

    public void skipBytes(int r) throws IOException {
        pos += 2 * (r - 1);
        offset = readFixed16();
    }

    public void skipBoolean(int r) throws IOException {
        if (bitCount == 0) {
            skip(r / 8);
            bitCount += r % 8;
        } else if ((bitCount + r) <= 8) {
            bitCount += r;
            bitCount /= 8;
        } else {
            pos++;
            r = r - (8 - bitCount);
            skip(r / 8);
            bitCount += r % 8;
        }
    }

    public boolean readBoolean() throws IOException {
        if (bitCount == 0)
            pos++;
        int bits = buf[pos - 1] & 0xff;
        int bit = (bits >> bitCount) & 1;
        bitCount++;
        if (bitCount == 8)
            bitCount = 0;
        return bit == 0 ? false : true;
    }

    public int readLength() throws IOException {
        bitCount = 0;
        if (runLength > 0) {
            runLength--; // in run
            return runValue;
        }

        int length = readFixed32();
        if (length >= 0) // not a run
            return length;

        runLength = (1 - length) >>> 1; // start of run
        runValue = (length + 1) & 1;
        return runValue;
    }

    public void skipLength(int r) throws IOException {
        skip(r * 4);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readFixed32());
    }

    public int readFixed32() throws IOException {
        int len = 1;
        if ((pos + 4) > buf.length)
            throw new EOFException();
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        pos += 4;
        return n;
    }

    public int readFixed16() throws IOException {
        int len = 1;
        if ((pos + 2) > buf.length)
            throw new EOFException();
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8);
        pos += 2;
        return n;
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readFixed64());
    }

    public long readFixed64() throws IOException {
        return (readFixed32() & 0xFFFFFFFFL) | (((long) readFixed32()) << 32);
    }

    public void skipNull() throws IOException {
        offset = readFixed16();
    }

    public byte[] readUnionFixed(int len) throws IOException {
        byte[] res = new byte[len];
        System.arraycopy(buf, offset + count, res, 0, len);
        offset = readFixed16();
        return res;
    }

    public String readString() throws IOException {
        int length = readFixed16();
        int len = length - offset;
        byte[] bytes = new byte[len];
        System.arraycopy(buf, offset + count, bytes, 0, len);
        offset = length;
        return UTF8.decode(ByteBuffer.wrap(bytes, 0, len)).toString();
    }

    public byte[] readBytes() throws IOException {
        int length = readFixed16();
        int len = length - offset;
        byte[] bytes = new byte[len];
        System.arraycopy(buf, offset + count, bytes, 0, len);
        offset = length;
        return bytes;
    }

    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        int length = readFixed16();
        int len = length - offset;
        ByteBuffer result;
        if (old != null && len <= old.capacity()) {
            result = old;
            result.clear();
        } else {
            result = ByteBuffer.allocate(len);
        }
        System.arraycopy(buf, offset + count, result.array(), result.position(), len);
        result.limit(len);
        offset = length;
        return result;
    }
}
