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

public class Q20_start {
    public static void main(String[] args) throws IOException {
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[3]; //l_shipdate
        String t2 = args[4];
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        int count = 0;
        GenericRecord r = reader.read();
        while (r != null) {
            String name = r.get("p_name").toString();
            if (name.startsWith(args[2])) {
                List<Record> psl = (List<Record>) r.get(1);
                for (Record ps : psl) {
                    if (Q20.lMatch((List<Record>) ps.get(2), t1, t2)) {
                        count++;
                    }
                }
            }
            r = reader.read();
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
    }
}
