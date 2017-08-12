package cores.core;

import java.io.IOException;

import org.apache.trevni.Input;

public class PageBuffer {
    private Input in;
    private long offset;
    private byte[] buf;
    private final static int PAGESIZE = 256 * 1024;

    public PageBuffer() {
        this.buf = new byte[PAGESIZE];
    }

    public void put(int bytePos, byte b) {
        buf[bytePos] = b;
    }

    public byte[] getBuffer() {
        return buf;
    }

    public void setBuffer(byte[] buffer) throws IOException {
        if (buffer.length != PAGESIZE) {
            throw new IOException("This buf doesn't have right length!");
        }
        this.buf = buffer;
    }

    public PageBuffer(Input in) throws IOException {
        this(in, 0);
    }

    public PageBuffer(Input in, long position) throws IOException {
        this.in = in;
        this.offset = position;
        this.buf = new byte[PAGESIZE];
        in.read(offset, buf, 0, buf.length);
    }

    public void pageActivate() throws IOException {
        in.read(offset, buf, 0, buf.length);
    }

    public byte read(int position) throws IOException {
        if (position < 0 || position >= PAGESIZE) {
            throw new IOException("The pos: " + position + "is out of page limit!");
        }
        return buf[position];
    }
}
