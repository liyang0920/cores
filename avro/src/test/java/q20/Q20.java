package q20;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class Q20 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        FilterOperator[] filters = new FilterOperator[2];
        //        int p_name = Integer.parseInt(args[3]);
        //        int i = 4;
        //        String[] com = new String[p_name];
        //        for (int m = 0; m < p_name; m++) {
        //            com[m] = args[i + m];
        //        }
        //        i += p_name;
        //        filters[0] = new Pfilter(com); //p_name
        //        filters[1] = new Lfilter(args[i], args[i + 1]); //l_shipdate
        filters[0] = new PfilterStart(args[3]); //p_name
        filters[1] = new Lfilter(args[4], args[5]); //l_shipdate
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        long t1 = System.currentTimeMillis();
        reader.filter();
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        int sumC = reader.getRowCount(0);
        while (reader.hasNext()) {
            Record r = reader.next();
            System.out.println(r.toString());
            count++;
        }
        //        reader.filterReadIO();
        //        long timeIO = reader.getTimeIO();
        //        int[] filterBlock = reader.getFilterBlock();
        //        int[] seekBlockRes = reader.getBlockSeekRes();
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println(sumC);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        //        System.out.println("IO time: " + timeIO);
        //        System.out.println("***********************filterBlockRes************");
        //        System.out.println("read Block: " + filterBlock[0] + "\tseeked Block: " + filterBlock[1] + "\tblock count: "
        //                + filterBlock[2]);
        //        System.out.println("***********************allBlockRes************");
        //        System.out.println("read Block: " + seekBlockRes[0] + "\tseeked Block: " + seekBlockRes[1] + "\tblock count: "
        //                + seekBlockRes[2]);
    }
}
