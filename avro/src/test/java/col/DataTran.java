package col;

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
    static void lSort(String path, String schema, int[] fields, String resultPath, int free, int mul)
            throws IOException {
        Schema l = new Schema.Parser().parse(new File(schema));
        List<Field> fs = l.getFields();
        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, l,
                free, mul);
        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(l);
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
            writer.append(new ComparableKey(data, fields), data);
        }
        reader.close();
        writer.flush();
        System.gc();
    }

    static void doublePri(String path1, String path2, String schema1, String schema2, int[] fIn1, int[] fIn2,
            int[] fOut, String resultPath, int free, int mul) throws IOException {
        Schema s1 = new Schema.Parser().parse(new File(schema1 + "single.avsc"));
        Schema s = new Schema.Parser().parse(new File(schema1 + "nest.avsc"));
        Schema s2 = new Schema.Parser().parse(new File(schema2));
        List<Field> fs1 = s1.getFields();

        BufferedReader reader1 = new BufferedReader(new FileReader(new File(path1)));
        SortedAvroReader reader2 = new SortedAvroReader(path2, s2, fIn2);

        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, s,
                free, mul);

        String line;
        Record r2 = reader2.next();
        ComparableKey k2 = new ComparableKey(r2, fIn2);
        while ((line = reader1.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                switch (fs1.get(i).schema().getType()) {
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
            ComparableKey k1 = new ComparableKey(data, fIn1);

            while (k2 != null && k1.compareTo(k2) > 0) {
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            List<Record> arr = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                arr.add(r2);
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr);
            writer.append(new ComparableKey(data, fOut), data);
        }
        reader1.close();
        while (reader2.hasNext()) {
            Record ff = reader2.next();
            ff.toString();
        }
        writer.flush();
        System.gc();
    }

    static void finalTran(String path1, String path2, String schema1, String schema2, int[] fIn1, int[] fIn2,
            String resultPath, int free, int mul) throws IOException {

        Schema s1 = new Schema.Parser().parse(new File(schema1 + "single.avsc"));
        Schema s = new Schema.Parser().parse(new File(schema1 + "nest.avsc"));
        Schema s2 = new Schema.Parser().parse(new File(schema2));
        List<Field> fs1 = s1.getFields();
        int mm = s2.getFields().size() - 1;

        BufferedReader reader1 = new BufferedReader(new FileReader(new File(path1)));
        SortedAvroReader reader2 = new SortedAvroReader(path2, s2, fIn2);

        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul);

        String line;
        Record r2 = reader2.next();
        ComparableKey k2 = new ComparableKey(r2, fIn2);
        //        int count = 0;
        while ((line = reader1.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                switch (fs1.get(i).schema().getType()) {
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
            ComparableKey k1 = new ComparableKey(data, fIn1);

            while (k2 != null && k1.compareTo(k2) > 0) {
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            List<Record> arr = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                arr.add(r2);
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr);
            writer.flush(data);
            //            count++;
            //            if (count >= 20)
            //                break;
        }
        reader1.close();
        reader2.close();
        int index = writer.flush();
        //        File[] files = new File[index];
        //        for (int i = 0; i < index; i++)
        //            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
        //        if (index == 1) {
        //            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
        //            new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
        //        } else {
        //            writer.mergeFiles(files);
        //        }
    }

    public static void main(String[] args) throws IOException {
        String path = args[0];
        String result = args[1] + "result";
        String schema = args[1] + "lay";
        int free = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);

        //        int[] fields0 = new int[] { 1, 2, 0, 3 };
        //        long start = System.currentTimeMillis();
        //        lSort(path + "lineitem.tbl", schema + "1/single.avsc", fields0, result + "1/", free, mul);
        //        long end = System.currentTimeMillis();
        //        System.out.println("+++++++lineitem sort time+++++++" + (end - start));

        //        int[] fields1 = new int[] { 0, 1 };
        //        int[] fields2 = new int[] { 1, 2 };
        //        int[] fields3 = new int[] { 0, 1 };
        //        start = System.currentTimeMillis();
        //        doublePri(path + "partsupp.tbl", result + "1/", schema + "2/", schema + "1/single.avsc", fields1, fields2,
        //                fields3, result + "2/", free, mul);
        //        end = System.currentTimeMillis();
        //        System.out.println("+++++++partsupp&&lineitem time+++++++" + (end - start));

        int[] fields4 = new int[] { 0 };
        int[] fields5 = new int[] { 0 };
        long start = System.currentTimeMillis();
        finalTran(path + "part.tbl", result + "2/", schema + "3/", schema + "2/nest.avsc", fields4, fields5,
                result + "3/", max, mul);
        long end = System.currentTimeMillis();
        System.out.println("+++++++part&&partsupp&&lineitem time+++++++" + (end - start));

        //        String resultPath = result + "3/";
        //        int index = 200;
        //        Schema s = new Schema.Parser().parse(new File(schema + "3/" + "nest.avsc"));
        //        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, max, mul);
        //        File[] files = new File[index];
        //        for (int i = 0; i < index; i++)
        //            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".neci");
        //        if (index == 1) {
        //            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
        //            new File(resultPath + "file0.neci").renameTo(new File(resultPath + "result.neci"));
        //        } else {
        //            writer.mergeFiles(files);
        //        }
        //        System.out.println("merge completed!");
    }
}
