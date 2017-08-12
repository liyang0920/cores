package cores.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

public class SortedAvroReader {
    private File[] files;
    private int numFiles;
    private Schema schema;
    private int[] sortKeyFields;
    private AvroReader[] readers;
    private Record[] sortedRecord;
    private int[] noR;
    private int start = 0;

    public SortedAvroReader(String path, Schema schema, int[] keyFields) throws IOException {
        this((new File(path)).listFiles(), schema, keyFields);
    }

    public SortedAvroReader(File[] files, Schema schema, int[] keyFields) throws IOException {
        this.files = files;
        this.numFiles = files.length;
        this.schema = schema;
        this.sortKeyFields = keyFields;
        create();
    }

    public void create() throws IOException {
        readers = new AvroReader[numFiles];
        sortedRecord = new Record[numFiles];
        noR = new int[numFiles];
        for (int i = 0; i < numFiles; i++) {
            readers[i] = new AvroReader(schema, files[i]);
            sortedRecord[i] = readers[i].next();
            noR[i] = i;
        }
        for (int i = 0; i < numFiles - 1; i++) {
            for (int j = i + 1; j < numFiles; j++) {
                CombKey k1 = new CombKey(sortedRecord[noR[i]], sortKeyFields);
                CombKey k2 = new CombKey(sortedRecord[noR[j]], sortKeyFields);
                if (k1.compareTo(k2) > 0) {
                    int tmpNo = noR[i];
                    noR[i] = noR[j];
                    noR[j] = tmpNo;
                }
            }
        }
    }

    public Record next() {
        Record r = sortedRecord[noR[start]];
        if (!readers[noR[start]].hasNext()) {
            start++;
        } else {
            sortedRecord[noR[start]] = readers[noR[start]].next();
            int m = start;
            CombKey key = new CombKey(sortedRecord[noR[start]], sortKeyFields);
            for (int i = start + 1; i < numFiles; i++) {
                if (key.compareTo(new CombKey(sortedRecord[noR[i]], sortKeyFields)) > 0) {
                    m++;
                } else {
                    break;
                }
            }
            if (m > start) {
                int tmpNo = noR[start];
                for (int i = start; i < m; i++) {
                    noR[i] = noR[i + 1];
                }
                noR[m] = tmpNo;
            }
        }
        return r;
    }

    public boolean hasNext() {
        return (start < numFiles);
    }

    public void close() throws IOException {
        for (AvroReader reader : readers) {
            reader.close();
        }
    }

    public static class AvroReader {
        private DataFileReader<Record> fileReader;

        public AvroReader(Schema schema, File file) throws IOException {
            DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
            fileReader = new DataFileReader<Record>(file, reader);
        }

        public boolean hasNext() {
            return fileReader.hasNext();
        }

        public Record next() {
            return fileReader.next();
        }

        public void close() throws IOException {
            fileReader.close();
        }
    }
}
