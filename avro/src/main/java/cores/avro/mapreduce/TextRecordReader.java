package cores.avro.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TextRecordReader extends RecordReader<Text, NullWritable> {
    private String mCurrentKey;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSplit file = (FileSplit) inputSplit;
        context.setStatus(file.toString());
        //        System.out.println("*************************neciFile" + file.getPath().toString());
        //        System.out.println("*************************headFile"
        //                + file.getPath().toString().substring(0, file.getPath().toString().lastIndexOf(".")) + ".head");
        mCurrentKey = file.getPath().toString();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return mCurrentKey != null;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        Text res = new Text(mCurrentKey);
        mCurrentKey = null;
        return res;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return mCurrentKey == null ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
        //null operator
    }
}
