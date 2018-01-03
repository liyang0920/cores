package local.avro.query;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

public class Q20 {
    static boolean contain(String s, String[] start) {
        for (int i = 0; i < start.length; i++) {
            if (s.contains(start[i]))
                return true;
        }
        return false;
    }

    static boolean lMatch(List<Record> l, String t1, String t2) {
        for (int i = 0; i < l.size(); i++) {
            String date = l.get(i).get("l_shipdate").toString();
            if (date.compareTo(t1) >= 0 && date.compareTo(t2) < 0)
                return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int p_name = Integer.parseInt(args[2]);
        int i = 3;
        String[] com = new String[p_name];
        for (int m = 0; m < p_name; m++) {
            com[m] = args[i + m];
        }
        i += p_name;
        String t1 = args[i]; //l_shipdate
        String t2 = args[i + 1];
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        int count = 0;
        while (fileReader.hasNext()) {
            Record r = fileReader.next();
            String name = r.get("p_name").toString();
            if (contain(name, com)) {
                List<Record> psl = (List<Record>) r.get(1);
                for (Record ps : psl) {
                    if (lMatch((List<Record>) ps.get(2), t1, t2)) {
                        count++;
                    }
                }
            }
        }
        fileReader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
    }
}
