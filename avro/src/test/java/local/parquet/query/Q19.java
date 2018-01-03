package local.parquet.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class Q19 {
    static boolean startWith(String s, String[] start) {
        for (int i = 0; i < start.length; i++) {
            if (s.startsWith(start[i]))
                return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String brandNo = args[2]; //p_brand
        int p_container = Integer.parseInt(args[3]);
        String[] com_p = new String[p_container]; //p_container
        int i = 4;
        for (int m = 0; m < p_container; m++) {
            com_p[m] = args[i + m];
        }
        i += p_container;
        int p_size = Integer.parseInt(args[i++]); //p_size
        float f1 = Float.parseFloat(args[i++]); //l_quantity
        float f2 = Float.parseFloat(args[i++]);
        int l_shipmode = Integer.parseInt(args[i++]);
        String[] com_l = new String[l_shipmode]; //l_shipmode
        for (int m = 0; m < l_shipmode; m++) {
            com_l[m] = args[i + m];
        }

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = new ParquetReader(conf, new Path(args[0]), readSupport);
        int count = 0;
        double result = 0.00;
        GenericRecord r = reader.read();
        while (r != null) {
            String brand = r.get("p_brand").toString();
            String contain = r.get("p_container").toString();
            int size = Integer.parseInt(r.get("p_size").toString());
            //            result += (float) r.get(0) * (1 - (float) r.get(1));
            if (brand.compareTo(brandNo) <= 0 && startWith(contain, com_p) && size >= 1 && size <= p_size) {
                List<Record> psl = (List<Record>) r.get(3);
                List<Record> l = new ArrayList<Record>();
                for (Record m : psl) {
                    l.addAll((List<Record>) m.get(0));
                }
                for (Record m : l) {
                    float quan = Float.parseFloat(m.get("l_quantity").toString());
                    String mode = m.get("l_shipmode").toString();
                    String instruct = m.get("l_shipinstruct").toString();
                    if (quan >= f1 && quan <= f2 && startWith(mode, com_l) && instruct.equals("DELIVER IN PERSON")) {
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
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }
}
