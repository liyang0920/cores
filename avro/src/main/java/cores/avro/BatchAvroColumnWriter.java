package cores.avro;

import static cores.avro.AvroColumnator.isSimple;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.util.Utf8;
import org.apache.trevni.TrevniRuntimeException;

import cores.core.BatchColumnFileWriter;
import cores.core.FileColumnMetaData;
import cores.core.FileMetaData;
import cores.core.InsertColumnFileWriter.ListArr;

public class BatchAvroColumnWriter<T> {
    private Schema schema;
    private BatchColumnFileWriter writer;
    private FileColumnMetaData[] meta;
    private FileMetaData filemeta;
    private List<T> values;
    private ListArr[] v;
    private String path;
    private int[] arrayWidths;
    private GenericData model;
    private long bytes;
    long start, end;

    private int fileIndex = 0;

    public static final String SCHEMA_KEY = "avro.schema";
    private int max;
    private int free;
    private int mul;

    public BatchAvroColumnWriter(Schema schema, String path, int free, int mul) throws IOException {
        this.schema = schema;
        AvroColumnator columnator = new AvroColumnator(schema);
        filemeta = new FileMetaData();
        filemeta.set(SCHEMA_KEY, schema.toString());
        this.meta = columnator.getColumns();
        this.writer = new BatchColumnFileWriter(filemeta, meta);
        this.arrayWidths = columnator.getArrayWidths();
        this.model = GenericData.get();
        //    this.numFiles = numFiles;
        //        this.max = max;
        this.max = free;
        this.mul = mul;
        //    fileDelete(path);
        this.path = path;
        bytes = 0;
        //    createFiles(path, numFiles);
        //        sort = new SortedArray<K, V>();
        values = new ArrayList<T>();
        v = new ListArr[meta.length];
        for (int k = 0; k < v.length; k++) {
            v[k] = new ListArr();
        }
        start = System.currentTimeMillis();
    }

    public void fileDelete(String path) {
        File file = new File(path);
        if (file.exists() & file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                files[i].delete();
                //                NestManager.shDelete(files[i].getAbsolutePath());
            }
        }
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void append(T value) throws IOException {
        values.add(value);
        //        if (fileIndex == 0) {
        //            bytes += value.toString().length();
        //            if (Runtime.getRuntime().freeMemory() <= (free * 1024 * 1024)) {
        //                max = values.size();
        //                System.out.println("####max####" + max);
        //                System.out.println("&&&&&&bytes&&&&\t" + bytes);
        //            } else {
        //                return;
        //            }
        //        } else {
        if (values.size() < max) {
            return;
        }
        //        }
        appendTo(new File(path + "file" + String.valueOf(fileIndex) + ".neci"));

        fileIndex++;
        end = System.currentTimeMillis();
        System.out.println("############" + (fileIndex) + "\ttime: " + (end - start) + "ms");
        System.out.println();
        start = System.currentTimeMillis();
        //    }
    }

    public void flush(T value) throws IOException {
        values.add(value);
        if (values.size() < max) {
            return;
        }
        flushTo(new File(path + "file" + String.valueOf(fileIndex) + ".neci"));

        fileIndex++;
        end = System.currentTimeMillis();
        System.out.println("############" + (fileIndex) + "\ttime: " + (end - start) + "ms");
        System.out.println();
        start = System.currentTimeMillis();
        //    }
    }

    public int flush() throws IOException {
        if (!values.isEmpty()) {
            //            if (fileIndex > 0) {
            //                appendTo(new File(path + "file" + String.valueOf(fileIndex) + ".neci"));
            //            } else {
            flushTo(new File(path + "file" + String.valueOf(fileIndex) + ".neci"));
            //            }
            fileIndex++;
            end = System.currentTimeMillis();
            System.out.println("Trevni#######" + (fileIndex) + "\ttime: " + (end - start) + "ms");
        }
        return fileIndex;
    }

    private int append(Object o, Schema s, int column) throws IOException {
        if (isSimple(s)) {
            appendValue(o, s, column);
            return column + 1;
        }
        switch (s.getType()) {
            case RECORD:
                for (Field f : s.getFields())
                    column = append(model.getField(o, f.name(), f.pos()), f.schema(), column);
                return column;
            case ARRAY:
                Collection elements = (Collection) o;
                appendValue(elements.size(), s, column);
                if (isSimple(s.getElementType())) { // optimize simple arrays
                    column++;
                    for (Object element : elements)
                        appendValue(element, s.getElementType(), column);
                    return column + 1;
                }
                for (Object element : elements) {
                    int c = append(element, s.getElementType(), column + 1);
                    assert (c == column + arrayWidths[column]);
                }
                return column + arrayWidths[column];
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    private void appendValue(Object o, Schema s, int column) throws IOException {
        switch (s.getType()) {
            case UNION:
                if (o != null && o instanceof Utf8)
                    o = o.toString();
                break;
            case STRING:
                if (o instanceof Utf8)
                    o = o.toString();
                break;
            case ENUM:
                if (o instanceof Enum)
                    o = ((Enum) o).ordinal();
                else
                    o = s.getEnumOrdinal(o.toString());
                break;
            case FIXED:
                o = ((GenericFixed) o).bytes();
                break;
        }
        v[column].add(o);
    }

    public void appendTo(File file) throws IOException {
        if (values.size() != 0) {
            while (values.size() > mul) {
                for (int i = 0; i < mul; i++) {
                    int count = append(values.get(i), schema, 0);
                    assert (count == meta.length);
                }
                values.subList(0, mul).clear();
            }
            for (T record : values) {
                int count = append(record, schema, 0);
                assert (count == meta.length);
            }
            values.clear();
        }
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (file.exists()) {
            file.delete();
            new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head").delete();
        }
        writer.setInsert(v);
        v = null;
        v = new ListArr[meta.length];
        for (int k = 0; k < v.length; k++) {
            v[k] = new ListArr();
        }
        long t1 = System.currentTimeMillis();
        writer.appendTo(file);
        long t2 = System.currentTimeMillis();
        System.out.println("@@@write time:  " + (t2 - t1));
        System.gc();
    }

    /*
     * write array column incremently
     */
    public void flushTo(File file) throws IOException {
        if (values.size() != 0) {
            while (values.size() > mul) {
                for (int i = 0; i < mul; i++) {
                    int count = append(values.get(i), schema, 0);
                    assert (count == meta.length);
                }
                values.subList(0, mul).clear();
            }
            for (T record : values) {
                int count = append(record, schema, 0);
                assert (count == meta.length);
            }
            values.clear();
        }
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (file.exists()) {
            file.delete();
            new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head").delete();
        }
        writer.setInsert(v);
        v = null;
        v = new ListArr[meta.length];
        for (int k = 0; k < v.length; k++) {
            v[k] = new ListArr();
        }
        long t1 = System.currentTimeMillis();
        writer.flushTo(file);
        long t2 = System.currentTimeMillis();
        System.out.println("@@@write time:  " + (t2 - t1));
        System.gc();
    }

    public void mergeFiles(File[] files) throws IOException {
        long t1 = System.currentTimeMillis();
        writer.setMergeFiles(files);
        writer.mergeFiles(new File(path + "result.neci"));
        for (File f : files) {
            f.delete();
            new File(f.getAbsolutePath().substring(0, f.getAbsolutePath().lastIndexOf(".")) + ".head").delete();
        }
        long t3 = System.currentTimeMillis();
        System.out.println("merge write time:\t" + (t3 - t1));
        System.gc();
    }
}
