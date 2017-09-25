package q19;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class Q19 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        String brandNo = args[3];
        int p_container = Integer.parseInt(args[4]);
        String[] com_p = new String[p_container];
        int i = 5;
        for (int m = 0; m < p_container; m++) {
            com_p[m] = args[i + m];
        }
        i += p_container;
        int p_size = Integer.parseInt(args[i++]);
        float f1 = Float.parseFloat(args[i++]);
        float f2 = Float.parseFloat(args[i++]);
        int l_shipmode = Integer.parseInt(args[i++]);
        String[] com_l = new String[l_shipmode];
        for (int m = 0; m < l_shipmode; m++) {
            com_l[m] = args[i + m];
        }
        FilterOperator[] filters = new FilterOperator[6];
        filters[0] = new Pfilter1(brandNo); //p_brand
        filters[1] = new Pfilter2(com_p); //p_container
        filters[2] = new Pfilter3(p_size); //p_size
        filters[3] = new Lfilter1(f1, f2); //l_quantity
        filters[4] = new Lfilter2(com_l); //l_shipmode
        filters[5] = new Lfilter3(); //l_shipinstruct
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        long t1 = System.currentTimeMillis();
        reader.filterNoCasc();
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        int sumC = reader.getRowCount(0);
        double result = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            result += (float) r.get(0) * (1 - (float) r.get(1));
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
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }
}
