package cores.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.trevni.avro.HadoopInput;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class NeciFilterRecordReader extends RecordReader<AvroKey<Record>, NullWritable> {
    private FilterBatchColumnReader<Record> reader;
    private float rows;
    private int row;
    private AvroKey<Record> mCurrentKey = new AvroKey<Record>();

    private FilterOperator[] filters;

    public NeciFilterRecordReader(FilterOperator[] filters) {
        this.filters = filters;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSplit file = (FileSplit) inputSplit;
        context.setStatus(file.toString());

        reader = new FilterBatchColumnReader<Record>(new HadoopInput(file.getPath(), context.getConfiguration()),
                new HadoopInput(new Path(
                        file.getPath().toString().substring(0, file.getPath().toString().lastIndexOf(".")) + ".head"),
                        context.getConfiguration()),
                filters);
        reader.createSchema(AvroJob.getInputKeySchema(context.getConfiguration()));
        reader.filter();
        reader.createFilterRead();
        rows = reader.getRowCount(0);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Record tm;
        if (!reader.hasNext())
            return false;
        row++;
        tm = reader.next();
        mCurrentKey.datum(tm);
        return true;
    }

    @Override
    public AvroKey<Record> getCurrentKey() throws IOException, InterruptedException {
        return mCurrentKey;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return row / rows;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
