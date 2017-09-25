package q14;

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

public class InputFormat_Q14 extends FileInputFormat<AvroKey<Record>, NullWritable> {
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
        filters[0] = new Lfilter(args[3], args[4]);
        return new NeciFilterRecordReader(filters);
    }
}
