package col;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.BatchAvroColumnWriter;

public class DataMerge {
    public static void main(String[] args) throws IOException {
        String result = args[0] + "result";
        String schema = args[0] + "lay";
        int mul = Integer.parseInt(args[1]);
        int max = Integer.parseInt(args[2]);
        int m = Integer.parseInt(args[3]);
        int n = Integer.parseInt(args[4]);
        int x = n / m;
        String resultPath = result + "3/";
        Schema s = new Schema.Parser().parse(new File(schema + "3/" + "nest.avsc"));
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, max, mul);
        for (int i = 0; i < x; i++) {
            File[] files = new File[m];
            int tm = i * m;
            for (int j = 0; j < m; j++) {
                files[j] = new File(resultPath + "file" + String.valueOf(tm + j) + ".neci");
            }
            writer.mergeFiles(files);
            new File(resultPath + "result.head").renameTo(new File(resultPath + "file" + String.valueOf(i) + ".head"));
            new File(resultPath + "result.neci").renameTo(new File(resultPath + "file" + String.valueOf(i) + ".neci"));
        }
        System.out.println("merge completed!");
    }
}
