package pubmed;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BlockSeek {
    public static void main(String[] args) throws IOException {
        String path = args[0];
        int blockSize = Integer.parseInt(args[1]) * 1024;
        RandomAccessFile reader = new RandomAccessFile(path, "rw");
        long res = 0;
        long length = reader.length();
        long i = 0;
        while (i < length) {
            res += Math.abs(reader.readLong());
            i += 8;
        }
        reader.close();
        res /= blockSize;
        System.out.println("********************" + path + "&&&&" + Integer.parseInt(args[1]) + "K");
        System.out.println("blocks number: " + res);
    }
}
