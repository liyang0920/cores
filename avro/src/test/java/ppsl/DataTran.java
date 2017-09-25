package ppsl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.BatchAvroColumnWriter;
import cores.avro.ComparableKey;
import cores.avro.SortedAvroReader;
import cores.avro.SortedAvroWriter;

public class DataTran {
    public static Record tranToRecord(String line, Schema s, List<Field> fs) {
        String[] tmp = line.split("\\|");
        Record data = new Record(s);
        for (int i = 0; i < fs.size(); i++) {
            switch (fs.get(i).schema().getType()) {
                case INT:
                    data.put(i, Integer.parseInt(tmp[i]));
                    break;
                case LONG:
                    data.put(i, Long.parseLong(tmp[i]));
                    break;
                case FLOAT:
                    data.put(i, Float.parseFloat(tmp[i]));
                    break;
                case DOUBLE:
                    data.put(i, Double.parseDouble(tmp[i]));
                    break;
                case BYTES:
                    data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                    break;
                default:
                    data.put(i, tmp[i]);
            }
        }
        return data;
    }

    static void lSort(String path, String schema, int[] fields, String resultPath, int free, int mul)
            throws IOException {
        Schema l = new Schema.Parser().parse(new File(schema));
        List<Field> fs = l.getFields();
        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, l,
                free, mul);
        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
        String line;
        while ((line = reader.readLine()) != null) {
            Record data = tranToRecord(line, l, fs);
            writer.append(new ComparableKey(data, fields), data);
        }
        reader.close();
        writer.flush();
        System.gc();
    }

    static void finalTran(String path1, String schema1, int[] fIn1, String path2, String schema2, int[] fIn2,
            String path3, String schema3, int[] fIn3, String resultPath, String schema, int free, int mul)
            throws IOException {

        Schema s1 = new Schema.Parser().parse(new File(schema1)); //part
        Schema s2 = new Schema.Parser().parse(new File(schema2)); //partsupp
        Schema s3 = new Schema.Parser().parse(new File(schema3)); //lineitem
        Schema s = new Schema.Parser().parse(new File(schema)); //p_ps_l
        List<Field> fs1 = s1.getFields();
        List<Field> fs2 = s2.getFields();

        BufferedReader reader1 = new BufferedReader(new FileReader(new File(path1)));
        BufferedReader reader2 = new BufferedReader(new FileReader(new File(path2)));
        SortedAvroReader reader3 = new SortedAvroReader(path3, s3, fIn3);

        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul);

        String line;

        String line2 = reader2.readLine();
        Record r2 = tranToRecord(line2, s2, fs2);
        ComparableKey k2 = new ComparableKey(r2, fIn2);

        Record r3 = reader3.next();
        ComparableKey k3 = new ComparableKey(r3, fIn3);
        while ((line = reader1.readLine()) != null) {
            Record data = tranToRecord(line, s, fs1);
            ComparableKey k1 = new ComparableKey(data, fIn1);

            List<Record> arr2 = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                arr2.add(r2);
                line2 = reader2.readLine();
                if (line2 != null) {
                    r2 = tranToRecord(line2, s2, fs2);
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr2);

            List<Record> arr3 = new ArrayList<Record>();
            while (k3 != null && k1.compareTo(k3) == 0) {
                arr3.add(r3);
                if (reader3.hasNext()) {
                    r3 = reader3.next();
                    k3 = new ComparableKey(r3, fIn3);
                } else {
                    k3 = null;
                    reader3.close();
                    break;
                }
            }
            data.put(fs1.size() + 1, arr3);
            writer.append(data);
        }
        reader1.close();
        int index = writer.flush();
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
        if (index == 1) {
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
        } else {
            writer.mergeFiles(files);
        }
    }

    public static void main(String[] args) throws IOException {
        String path = args[0];
        String result = args[1];
        int free = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);

        int[] f0 = new int[] { 1, 0, 3 };
        lSort(path + "lineitem.tbl", result + "lineitem.avsc", f0, result + "tmp/", free, mul);

        int[] f1 = new int[] { 0 };
        int[] f2 = new int[] { 0 };
        int[] f3 = new int[] { 1 };
        finalTran(path + "part.tbl", result + "part.avsc", f1, path + "partsupp.tbl", result + "partsupp.avsc", f2,
                result + "tmp/", result + "lineitem.avsc", f3, result + "result/", result + "p_ps_l.avsc", free, mul);
    }
}
