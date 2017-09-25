package q6;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class Q6 {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        FilterOperator[] filters = new FilterOperator[3];
        filters[2] = new Lfilter1(args[3], args[4]); //l_shipdate
        filters[1] = new Lfilter2(Float.parseFloat(args[5]), Float.parseFloat(args[6])); //l_discount
        filters[0] = new Lfilter3(Float.parseFloat(args[7])); //l_quantity
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
            result += (float) r.get(0) * (float) r.get(1);
            count++;
        }
        //        reader.filterReadIO();
        //        long timeIO = reader.getTimeIO();
        //        List<Long> blockTime = reader.getBlockTime();
        //        List<Long> blockStart = reader.getBlockStart();
        //        List<Long> blockEnd = reader.getBlockEnd();
        //        List<Long> blockOffset = reader.getBlockOffset();
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
        //        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        //        nf.setGroupingUsed(false);
        //        System.out.println("revenue: " + nf.format(result));
        //        System.out.println("blockOffset length: " + blockOffset.size() + "\tstart length: " + blockStart.size()
        //                + "\tend length: " + blockEnd.size() + "\ttime length: " + blockTime.size());
        //        long timeForIO = 0;
        //        for (int i = 0; i < blockTime.size(); i++) {
        //            timeForIO += blockTime.get(i);
        //        }
        //        System.out.println("timeForIO: " + timeForIO);
        //        for (int i = 0; i < blockTime.size(); i++) {
        //            System.out.println("blockOffset: " + blockOffset.get(i) + "\tstart: " + blockStart.get(i) + "\tend: "
        //                    + blockEnd.get(i) + "\ttime: " + blockTime.get(i));
        //        }
    }
}
