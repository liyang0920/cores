package q19;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import cores.avro.FilterOperator;
import cores.avro.mapreduce.NeciFilterRecordReader;

public class InputFormat_Q19 extends FileInputFormat<AvroKey<Record>, NullWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext context) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        //        context.setBoolean("mapred.input.dir.recursive", true);
        for (FileStatus file : super.listStatus(context))
            if (file.getPath().getName().endsWith(".neci"))
                result.add(file);
        return result;
    }

    @Override
    public RecordReader<AvroKey<Record>, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FilterOperator[] filters = new FilterOperator[1];
        Configuration conf = context.getConfiguration();
        String[] args = conf.getStrings("args");
        Integer brandNo = Integer.parseInt(args[3]);
        //        int p_container = Integer.parseInt(args[4]);
        //        String[] com_p = new String[p_container];
        //        int i = 5;
        //        for (int m = 0; m < p_container; m++) {
        //            com_p[m] = args[i + m];
        //        }
        //        i += p_container;
        //        int p_size = Integer.parseInt(args[i++]);
        //        float f1 = Float.parseFloat(args[i++]);
        //        float f2 = Float.parseFloat(args[i++]);
        //        int l_shipmode = Integer.parseInt(args[i++]);
        //        String[] com_l = new String[l_shipmode];
        //        for (int m = 0; m < l_shipmode; m++) {
        //            com_l[m] = args[i + m];
        //        }
        filters[0] = new Pfilter(brandNo); //p_brand
        //        filters[1] = new Pfilter2(com_p); //p_container
        //        filters[2] = new Pfilter3(p_size); //p_size
        //        filters[3] = new Lfilter1(f1, f2); //l_quantity
        //        filters[4] = new Lfilter2(com_l); //l_shipmode
        //        filters[5] = new Lfilter3(); //l_shipinstruct
        return new NeciFilterRecordReader(filters);
    }
}
