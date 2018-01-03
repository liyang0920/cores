package local.trevni.query;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

public class PubQ4 {
    static boolean equal(String[] com, String s) {
        for (int i = 0; i < com.length; i++) {
            if (s.equals(com[i]))
                return true;
        }
        return false;
    }

    static boolean contain(String[] word, String s) {
        for (int i = 0; i < word.length; i++) {
            if (s.contains(word[i]))
                return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2];
        String t2 = args[3];

        int name = Integer.parseInt(args[4]);
        int i = 5;
        String[] com_name = new String[name];
        for (int m = 0; m < name; m++) {
            com_name[m] = args[i + m];
        }

        //        i += name;
        //        int word = Integer.parseInt(args[i++]);
        //        String[] com_key = new String[word];
        //        for (int m = 0; m < word; m++) {
        //            com_key[m] = args[i + m];
        //        }

        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(readSchema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);
        int count = 0;
        while (reader.hasNext()) {
            Record r = (Record) reader.next().get("MedlineCitation");
            String dateCom = r.get("DateCompleted").toString();
            if (dateCom.compareTo(t1) >= 0 && dateCom.compareTo(t2) <= 0) {
                String owner = r.get("Owner").toString();
                boolean tr = equal(com_name, owner);
                //                if (tr) {
                //                    List<Record> kl = (List<Record>) r.get("KeywordList");
                //                    for (Record k : kl) {
                //                        String key = k.get("keyword").toString();
                //                        if (contain(com_key, key)) {
                //                            break;
                //                        }
                //                        tr = false;
                //                    }
                //                    if (kl.isEmpty())
                //                        tr = false;
                //                }
                if (tr)
                    count++;
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
    }
}
