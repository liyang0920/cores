package cores.core;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.trevni.TrevniRuntimeException;

public class BlockOutputBuffer {
    static final int BLOCK_SIZE = 32 * 1024;
    static final int COUNT = 32 * 1024;

    protected int bitCount; // position in booleans

    protected byte[] buf1;
    protected byte[] buf2;
    protected int count1;
    protected int count2;

    public BlockOutputBuffer() {
        buf1 = new byte[BLOCK_SIZE];
        buf2 = new byte[BLOCK_SIZE];
    }

    public boolean isFull() {
        return (count1 + count2) >= BLOCK_SIZE;
    }

    public int size() {
        return count1 + count2;
    }

    public void close() {
        buf1 = null;
        buf2 = null;
    }

    public void writeValue(Object value, ValueType type) throws IOException {
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                writeBoolean((Boolean) value);
                break;
            case INT:
                writeFixed32((Integer) value);
                break;
            case LONG:
                writeFixed64((Long) value);
                break;
            case FIXED32:
                writeFixed32((Integer) value);
                break;
            case FIXED64:
                writeFixed64((Long) value);
                break;
            case FLOAT:
                writeFloat((Float) value);
                break;
            case DOUBLE:
                writeDouble((Double) value);
                break;
            case STRING:
                writeString((String) value);
                break;
            case BYTES:
                if (value instanceof ByteBuffer)
                    writeBytes((ByteBuffer) value);
                else
                    writeBytes((byte[]) value);
                break;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    protected void ensureB1() {
        if (count1 == BLOCK_SIZE)
            buf1 = Arrays.copyOf(buf1, (buf1.length << 1));
    }

    protected void ensure(int n) {
        if (count2 + n > buf2.length)
            buf2 = Arrays.copyOf(buf2, Math.max(buf2.length << 1, count2 + n));
    }

    public void writeBoolean(boolean value) {
        if (bitCount == 0) { // first bool in byte
            ensure(1);
            count2++;
        }
        if (value)
            buf2[count2 - 1] |= (byte) (1 << bitCount);
        bitCount++;
        if (bitCount == 8)
            bitCount = 0;
    }

    public void writeLength(int length) throws IOException {
        writeFixed32(length);
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    public void writeString(String string) throws IOException {
        byte[] bytes = string.getBytes(UTF8);
        write(bytes, 0, bytes.length);
        buf1[count1] = (byte) ((count2) & 0xFF);
        buf1[count1 + 1] = (byte) ((count2 >>> 8) & 0xFF);
        count1 += 2;
    }

    public void writeBytes(ByteBuffer bytes) throws IOException {
        int pos = bytes.position();
        int start = bytes.arrayOffset() + pos;
        int len = bytes.limit() - pos;
        writeBytes(bytes.array(), start, len);
    }

    public void writeBytes(byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }

    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
        write(bytes, start, len);
        buf1[count1] = (byte) ((count2) & 0xFF);
        buf1[count1 + 1] = (byte) ((count2 >>> 8) & 0xFF);
        count1 += 2;
    }

    public void writeFloat(float f) throws IOException {
        writeFixed32(Float.floatToRawIntBits(f));
    }

    public void writeDouble(double d) throws IOException {
        writeFixed64(Double.doubleToRawLongBits(d));
    }

    public void writeFixed32(int i) throws IOException {
        ensure(4);
        buf2[count2] = (byte) ((i) & 0xFF);
        buf2[count2 + 1] = (byte) ((i >>> 8) & 0xFF);
        buf2[count2 + 2] = (byte) ((i >>> 16) & 0xFF);
        buf2[count2 + 3] = (byte) ((i >>> 24) & 0xFF);
        count2 += 4;
    }

    public void writeFixed64(long l) throws IOException {
        ensure(8);
        int first = (int) (l & 0xFFFFFFFF);
        int second = (int) ((l >>> 32) & 0xFFFFFFFF);
        buf2[count2] = (byte) ((first) & 0xFF);
        buf2[count2 + 4] = (byte) ((second) & 0xFF);
        buf2[count2 + 5] = (byte) ((second >>> 8) & 0xFF);
        buf2[count2 + 1] = (byte) ((first >>> 8) & 0xFF);
        buf2[count2 + 2] = (byte) ((first >>> 16) & 0xFF);
        buf2[count2 + 6] = (byte) ((second >>> 16) & 0xFF);
        buf2[count2 + 7] = (byte) ((second >>> 24) & 0xFF);
        buf2[count2 + 3] = (byte) ((first >>> 24) & 0xFF);
        count2 += 8;
    }

    public synchronized void writeTo(OutputStream out) throws IOException {
        out.write(buf1, 0, count1);
        out.write(buf2, 0, count2);
    }

    public synchronized void reset() {
        count1 = 0;
        count2 = 0;
    }

    public synchronized void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        ensure(len);
        System.arraycopy(b, off, buf2, count2, len);
        count2 += len;
    }
}
