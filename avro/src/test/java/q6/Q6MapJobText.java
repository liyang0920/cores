package q6;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class Q6MapJobText extends Configured implements Tool {
    public static class myMap extends Mapper<Text, NullWritable, NullWritable, Text> {
        private FilterOperator[] filters;
        private Schema schema;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            schema = AvroJob.getInputKeySchema(context.getConfiguration());
            String[] args = conf.getStrings("args");
            filters = new FilterOperator[3];
            filters[2] = new Lfilter1(args[3], args[4]); //l_shipdate
            filters[1] = new Lfilter2(Float.parseFloat(args[5]), Float.parseFloat(args[6])); //l_discount
            filters[0] = new Lfilter3(Float.parseFloat(args[7])); //l_quantity
        }

        @Override
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            double revenue = 0.00;
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
                Record r = reader.next();
                count++;
                revenue += (float) r.get("l_discount") * (float) r.get("l_extendedprice");
            }
            reader.close();
            context.write(NullWritable.get(), new Text(count + "|" + revenue));
        }
    }

    public static class myReduce extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double revenue = 0.00;
            int no = 0;
            for (Text value : values) {
                String[] tmp = value.toString().split("\\|");
                no += Integer.parseInt(tmp[0]);
                revenue += Double.parseDouble(tmp[1]);
            }
            java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
            nf.setGroupingUsed(false);
            context.write(NullWritable.get(), new Text(no + " | " + nf.format(revenue)));
        }
    }

    public static class myCombiner extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double revenue = 0.00;
            int no = 0;
            for (Text value : values) {
                String[] tmp = value.toString().split("\\|");
                no += Integer.parseInt(tmp[0]);
                revenue += Double.parseDouble(tmp[1]);
            }
            java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
            nf.setGroupingUsed(false);
            context.write(key, new Text(no + "|" + nf.format(revenue)));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("args", args);
        int pathNum = Integer.parseInt(args[0]);
        Schema inputSchema = new Schema.Parser().parse(new File(args[1]));
        String result = args[2];
        int i = 8;

        Job job = new Job(conf, "Q6MapJobText");
        job.setJarByClass(Q6MapJobText.class);

        AvroJob.setInputKeySchema(job, inputSchema);

        job.setMapperClass(myMap.class);
        job.setReducerClass(myReduce.class);
        job.setCombinerClass(myCombiner.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        for (int m = 0; m < pathNum; m++) {
            FileInputFormat.addInputPath(job, new Path(args[i + m]));
        }
        FileOutputFormat.setOutputPath(job, new Path(result));

        job.setInputFormatClass(InputFormatText.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q6MapJobText(), args);
        System.exit(res);
    }
}
