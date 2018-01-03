package local.trevni.query;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

public class PubQ3 {
    static boolean contain(String[] word, String s) {
        for (int i = 0; i < word.length; i++) {
            if (s.contains(word[i]))
                return true;
        }
        return false;
    }

    static boolean start(String[] name, String s) {
        for (int i = 0; i < name.length; i++) {
            if (s.startsWith(name[i]))
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
            String dateCr = r.get("DateCreated").toString();
            if (dateCr.compareTo(t1) >= 0 && dateCr.compareTo(t2) <= 0) {
                boolean tr = false;
                List<Record> aul = (List<Record>) ((Record) r.get("Article")).get("AuthorList");
                for (Record au : aul) {
                    String ln = au.get("AuLastName").toString();
                    if (start(com_name, ln)) {
                        tr = true;
                        break;
                    }
                }
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
