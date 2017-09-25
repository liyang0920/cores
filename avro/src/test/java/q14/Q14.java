package q14;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class Q14 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        FilterOperator[] filters = new FilterOperator[1];
        filters[0] = new Lfilter(args[3], args[4]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        long t1 = System.currentTimeMillis();
        reader.filter();
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            //            List<Record> psL = (List<Record>) r.get(1);
            //            for (Record m : psL) {
            //                List<Record> l = (List<Record>) m.get(0);
            //                count += l.size();
            //            }
            if (r.get(0).toString().startsWith("PROMO")) {
                List<Record> psL = (List<Record>) r.get(1);
                for (Record ps : psL) {
                    List<Record> lL = (List<Record>) ps.get(0);
                    count += lL.size();
                    for (Record l : lL) {
                        double res = (float) l.get(0) * (1 - (float) l.get(1));
                        sum += res;
                        result += res;
                    }
                }
            } else {
                List<Record> psL = (List<Record>) r.get(1);
                for (Record ps : psL) {
                    List<Record> lL = (List<Record>) ps.get(0);
                    count += lL.size();
                    for (Record l : lL) {
                        double res = (float) l.get(0) * (1 - (float) l.get(1));
                        sum += res;
                    }
                }
            }
        }
        //        reader.filterReadIO();
        //        long timeIO = reader.getTimeIO();
        //        int[] filterBlock = reader.getFilterBlock();
        //        int[] seekBlockRes = reader.getBlockSeekRes();
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        //        System.out.println("IO time: " + timeIO);
        //        System.out.println("***********************filterBlockRes************");
        //        System.out.println("read Block: " + filterBlock[0] + "\tseeked Block: " + filterBlock[1] + "\tblock count: "
        //                + filterBlock[2]);
        //        System.out.println("***********************allBlockRes************");
        //        System.out.println("read Block: " + seekBlockRes[0] + "\tseeked Block: " + seekBlockRes[1] + "\tblock count: "
        //                + seekBlockRes[2]);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }
}
