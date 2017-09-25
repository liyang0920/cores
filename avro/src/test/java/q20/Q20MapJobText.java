package q20;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.trevni.avro.HadoopInput;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;
import cores.avro.mapreduce.InputFormatText;

public class Q20MapJobText extends Configured implements Tool {
    public static class myMap extends Mapper<Text, NullWritable, NullWritable, IntWritable> {
        private FilterOperator[] filters;
        private Schema schema;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            schema = AvroJob.getInputKeySchema(context.getConfiguration());
            String[] args = conf.getStrings("args");
            filters = new FilterOperator[2];
            //            int p_name = Integer.parseInt(args[3]);
            //            int i = 4;
            //            String[] com = new String[p_name];
            //            for (int m = 0; m < p_name; m++) {
            //                com[m] = args[i + m];
            //            }
            //            i += p_name;
            //            filters[0] = new Pfilter(com); //p_name
            //            filters[1] = new Lfilter(args[i], args[i + 1]); //l_shipdate
            filters[0] = new PfilterStart(args[3]); //p_name
            filters[1] = new Lfilter(args[4], args[5]); //l_shipdate
        }

        @Override
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            int count = 0;
            String path = key.toString();
            FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(
                    new HadoopInput(new Path(path), context.getConfiguration()),
                    new HadoopInput(new Path(path.substring(0, path.lastIndexOf(".")) + ".head"),
                            context.getConfiguration()),
                    filters);
            reader.createSchema(schema);
            reader.filter();
            reader.createFilterRead();
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
            reader.close();
            context.write(NullWritable.get(), new IntWritable(count));

        }
    }

    public static class myReduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(NullWritable.get(), new IntWritable(count));
        }
    }

    public static class myCombiner extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("args", args);
        int pathNum = Integer.parseInt(args[0]);
        Schema inputSchema = new Schema.Parser().parse(new File(args[1]));
        String result = args[2];
        int i = 6;
        //        i += Integer.parseInt(args[3]);

        Job job = new Job(conf, "Q20MapJobText");
        job.setJarByClass(Q20MapJobText.class);

        AvroJob.setInputKeySchema(job, inputSchema);

        job.setMapperClass(myMap.class);
        job.setReducerClass(myReduce.class);
        job.setCombinerClass(myCombiner.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        for (int m = 0; m < pathNum; m++) {
            FileInputFormat.addInputPath(job, new Path(args[i + m]));
        }
        FileOutputFormat.setOutputPath(job, new Path(result));

        job.setInputFormatClass(InputFormatText.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q20MapJobText(), args);
        System.exit(res);
    }
}
