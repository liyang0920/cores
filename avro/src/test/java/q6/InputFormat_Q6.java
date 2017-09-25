package q6;

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

public class InputFormat_Q6 extends FileInputFormat<AvroKey<Record>, NullWritable> {
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
        FilterOperator[] filters = new FilterOperator[3];
        Configuration conf = context.getConfiguration();
        String[] args = conf.getStrings("args");
        filters[2] = new Lfilter1(args[3], args[4]); //l_shipdate
        filters[1] = new Lfilter2(Float.parseFloat(args[5]), Float.parseFloat(args[6])); //l_discount
        filters[0] = new Lfilter3(Float.parseFloat(args[7])); //l_quantity
        return new NeciFilterRecordReader(filters);
    }
}
