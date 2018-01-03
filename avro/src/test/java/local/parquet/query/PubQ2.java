package local.parquet.query;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class PubQ2 {
    static boolean equal(String[] com, String s) {
        for (int i = 0; i < com.length; i++) {
            if (s.equals(com[i]))
                return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2];
        String t2 = args[3];
        int co = Integer.parseInt(args[4]);
        int i = 5;
        String[] com_Gr = new String[co];
        for (int m = 0; m < co; m++) {
            com_Gr[m] = args[i + m];
        }

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        int count = 0;
        Record r = (Record) reader.read().get("MedlineCitation");
        while (r != null) {
            String dateCom = r.get("DateCompleted").toString();
            if (dateCom.compareTo(t1) >= 0 && dateCom.compareTo(t2) <= 0) {
                List<Record> gl = (List<Record>) ((Record) r.get("Article")).get("GrantList");
                for (Record g : gl) {
                    String country = g.get("GrCountry").toString();
                    if (equal(com_Gr, country)) {
                        count++;
                    }
                }
            }
            GenericRecord record = reader.read();
            if (record == null)
                r = null;
            else
                r = (Record) record.get("MedlineCitation");
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
    }
}
