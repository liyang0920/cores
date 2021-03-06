package local.avro.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;;

public class Q6 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String t1 = args[2]; //l_shipdate
        String t2 = args[3];
        float d1 = Float.parseFloat(args[4]); //l_discount
        float d2 = Float.parseFloat(args[5]);
        float q = Float.parseFloat(args[6]); //l_quantity
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        int count = 0;
        double result = 0.00;
        while (fileReader.hasNext()) {
            Record r = fileReader.next();
            List<Record> psl = (List<Record>) r.get(0);
            List<Record> l = new ArrayList<Record>();
            for (Record m : psl) {
                l.addAll((List<Record>) m.get(0));
            }
            for (Record m : l) {
                String date = m.get("l_shipdate").toString();
                float dis = Float.parseFloat(m.get("l_discount").toString());
                float quan = Float.parseFloat(m.get("l_quantity").toString());
                //            result += (float) r.get(0) * (float) r.get(1);
                if (date.compareTo(t1) >= 0 && date.compareTo(t2) < 0 && dis >= d1 && dis <= d2 && quan < q)
                    count++;
            }
        }
        fileReader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }
}
