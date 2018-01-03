package local.avro.query;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

public class Q14 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        int count = 0;
        //        double result = 0.00;
        //        double sum = 0.00;
        while (fileReader.hasNext()) {
            Record r = fileReader.next();
            List<Record> psL = (List<Record>) r.get(1);
            for (Record m : psL) {
                List<Record> l = (List<Record>) m.get(0);
                for (Record n : l) {
                    String date = n.get("l_shipdate").toString();
                    if (date.compareTo(args[2]) >= 0 && date.compareTo(args[3]) < 0) {
                        count++;
                    }
                }
            }
            //            if (r.get(0).toString().startsWith("PROMO")) {
            //                List<Record> psL = (List<Record>) r.get(1);
            //                for (Record ps : psL) {
            //                    List<Record> lL = (List<Record>) ps.get(0);
            //                    for (Record l : lL) {
            //                        double res = (float) l.get(0) * (1 - (float) l.get(1));
            //                        sum += res;
            //                        result += res;
            //                    }
            //                }
            //            } else {
            //                List<Record> psL = (List<Record>) r.get(1);
            //                for (Record ps : psL) {
            //                    List<Record> lL = (List<Record>) ps.get(0);
            //                    for (Record l : lL) {
            //                        double res = (float) l.get(0) * (1 - (float) l.get(1));
            //                        sum += res;
            //                    }
            //                }
            //            }
        }
        fileReader.close();
        //        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        //        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        //        nf.setGroupingUsed(false);
        //        System.out.println("revenue: " + nf.format(result));
    }
}
