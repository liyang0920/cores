package pubmed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Util {
    static BufferedInputStream fileOrStdin(String filename, InputStream stdin) throws IOException {
        return new BufferedInputStream(filename.equals("-") ? stdin : openFromFS(filename));
    }

    static BufferedOutputStream fileOrStdout(String filename, OutputStream stdout) throws IOException {
        return new BufferedOutputStream(filename.equals("-") ? stdout : createFromFS(filename));
    }

    static OutputStream createFromFS(String filename) throws IOException {
        Path p = new Path(filename);
        return new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p));
    }

    static InputStream openFromFS(String filename) throws IOException {
        Path p = new Path(filename);
        return p.getFileSystem(new Configuration()).open(p);
    }

    static void close(InputStream in) {
        if (!System.in.equals(in)) {
            try {
                in.close();
            } catch (IOException e) {
                System.err.println("could not close InputStream " + in.toString());
            }
        }
    }
}
