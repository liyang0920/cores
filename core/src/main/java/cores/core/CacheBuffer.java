package cores.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.trevni.Input;
import org.apache.trevni.InputFile;

public class CacheBuffer implements Closeable {
    private final static int PAGESIZE = 256 * 1024;
    private int numHashes;
    private int numElements;
    private int numPages;
    private long numBits;
    private PageBuffer[] pages;
    private Input file;

    public CacheBuffer(File file) throws IOException {
        this(new InputFile(file));
    }

    public CacheBuffer(Input file) throws IOException {
        byte[] buf = new byte[24];
        this.file = file;
        file.read(0, buf, 0, buf.length);
        this.numHashes = ((int) buf[0] & 0xff) | (((int) buf[1] & 0xff) << 8) | (((int) buf[2] & 0xff) << 16)
                | (((int) buf[3] & 0xff) << 24);
        this.numElements = ((int) buf[4] & 0xff) | (((int) buf[5] & 0xff) << 8) | (((int) buf[6] & 0xff) << 16)
                | (((int) buf[7] & 0xff) << 24);
        this.numPages = ((int) buf[8] & 0xff) | (((int) buf[9] & 0xff) << 8) | (((int) buf[10] & 0xff) << 16)
                | (((int) buf[11] & 0xff) << 24);
        this.numBits = ((long) buf[12] & 0xff) | (((long) buf[13] & 0xff) << 8) | (((long) buf[14] & 0xff) << 16)
                | (((long) buf[15] & 0xff) << 24) | (((long) buf[16] & 0xff) << 32) | (((long) buf[17] & 0xff) << 40)
                | (((long) buf[18] & 0xff) << 48) | (((long) buf[19] & 0xff) << 56);
        long pos = 20;
        pages = new PageBuffer[numPages];
        for (int i = 0; i < numPages; i++) {
            pages[i] = new PageBuffer(file, pos);
            //pages[i].pageActivate();
            pos += PAGESIZE;
        }
    }

    public int getNumHashes() {
        return numHashes;
    }

    public int getNumElements() {
        return numElements;
    }

    public int getNumPages() {
        return numPages;
    }

    public long getNumBits() {
        return numBits;
    }

    public static int getPageSize() {
        return PAGESIZE;
    }

    public PageBuffer getPage(int i) {
        return pages[i];
    }

    public byte read(long numbyte) throws IOException {
        int pageNo = (int) numbyte / PAGESIZE;
        int bytePos = (int) numbyte % PAGESIZE;
        return pages[pageNo].read(bytePos);
    }

    public void close() throws IOException {
        file.close();
        pages = null;
    }
}
