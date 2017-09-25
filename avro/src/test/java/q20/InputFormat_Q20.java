package q20;

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

public class InputFormat_Q20 extends FileInputFormat<AvroKey<Record>, NullWritable> {
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
        FilterOperator[] filters = new FilterOperator[2];
        Configuration conf = context.getConfiguration();
        String[] args = conf.getStrings("args");
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
        return new NeciFilterRecordReader(filters);
    }
}
