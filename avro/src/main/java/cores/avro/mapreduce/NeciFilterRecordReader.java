package cores.avro.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    private static final Log LOG = LogFactory.getLog(NeciFilterRecordReader.class);

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
        System.out.println("*************************neciFile" + file.getPath().toString());
        System.out.println("*************************headFile"
                + file.getPath().toString().substring(0, file.getPath().toString().lastIndexOf(".")) + ".head");
        LOG.info("*************************neciFile" + file.getPath().toString());
        LOG.info("*************************headFile"
                + file.getPath().toString().substring(0, file.getPath().toString().lastIndexOf(".")) + ".head");

        reader = new FilterBatchColumnReader<Record>(new HadoopInput(file.getPath(), context.getConfiguration()),
                new HadoopInput(new Path(
                        file.getPath().toString().substring(0, file.getPath().toString().lastIndexOf(".")) + ".head"),
                        context.getConfiguration()),
                filters);
        reader.createSchema(AvroJob.getInputKeySchema(context.getConfiguration()));
        reader.filter();
        System.out.println("Filter set ***" + reader.getCurrentSet());
        LOG.info("Filter set ***" + reader.getCurrentSet().toString());
        reader.createFilterRead();
        ArrayList<BitSet> set = reader.getReadSet();
        for (int i = 0; i < set.size(); i++) {
            System.out.println(i + "***" + set.get(i));
            LOG.info(i + "***" + set.get(i).toString());
        }
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
