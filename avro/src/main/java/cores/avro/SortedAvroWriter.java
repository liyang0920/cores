package cores.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

public class SortedAvroWriter<K, V> {
    private String path;
    //  private int numFiles;
    //  private File[] files;
    //  private File tmpFile;
    private Schema schema;
    //  private int[] wTimes;
    private SortedArray<K, V> sort = new SortedArray<K, V>();
    private int x = 0;
    private long bytes;

    private int fileIndex = 0;
    private int max;
    private int free;
    private int mul;

    public SortedAvroWriter(String path, Schema schema, int free, int mul) {
        //    assert numFiles > 1;
        this.path = path;
        this.free = free;
        this.mul = mul;
        bytes = 0;
        //    wTimes = new int[numFiles];
        fileDelete(path);
        //    createFiles(path, numFiles);
        this.schema = schema;
    }

    public String getPath() {
        return path;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public void fileDelete(String path) {
        File file = new File(path);
        if (file.exists() & file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                files[i].delete();
            }
        }
    }

    //  public void createFiles(String path, int no){
    //    files = new File[no];
    //    for(int i = 0; i < no; i++){
    //      files[i] = new File(path+"file"+String.valueOf(i)+".avro");
    //    }
    //    tmpFile = new File(path + "readtmp");
    //  }

    public void append(K key, V value) throws IOException {
        //    if(!schema.equals(record.getSchema())){
        //      throw new IOException("This record does not match the writer schema!!");
        //    }
        sort.put(key, value);
        if (x == 0) {
            bytes += value.toString().length();
            if (Runtime.getRuntime().freeMemory() <= (free * 1024 * 1024)) {
                max = sort.size();
                mul = max / mul;
                System.out.println("####sortarray max####" + mul);
                System.out.println("####max####" + max);
                System.out.println("&&&&bytes&&&&\t" + bytes);
                writeToFile();
            }
        } else {
            if (sort.size() == max) {
                writeToFile();
            }
        }
    }

    public void flush() throws IOException {
        if (!sort.isEmpty()) {
            writeToFile();
        }
    }

    public void writeToFile() throws IOException {
        long start = System.currentTimeMillis();
        DatumWriter<V> writer = new GenericDatumWriter<V>(schema);
        DataFileWriter<V> fileWriter = new DataFileWriter<V>(writer);
        File file = new File(path + "file" + String.valueOf(fileIndex) + ".avro");
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        fileWriter.create(schema, file);
        while (sort.size() != 0) {
            for (V record : sort.values(mul)) {
                fileWriter.append(record);
            }
        }
        fileWriter.close();
        //      wTimes[fileIndex]++;
        fileIndex++;
        //    }
        long end = System.currentTimeMillis();
        System.out.println("Avro#######" + (++x) + "\ttime: " + (end - start) + "ms");
        System.gc();
    }
}
