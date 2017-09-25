package q14;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;
import cores.avro.mapreduce.InputFormatText;

public class Q14MapJobLocal extends Configured implements Tool {
    private final static Log LOG = LogFactory.getLog(Q14MapJobLocal.class);

    public static class myMap extends Mapper<Text, NullWritable, NullWritable, Text> {
        private FilterOperator[] filters;
        private Schema schema;
        private List<File> paths;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            schema = AvroJob.getInputKeySchema(context.getConfiguration());
            String[] args = conf.getStrings("args");
            filters = new FilterOperator[1];
            filters[0] = new Lfilter(args[3], args[4]);

            File file = new File(args[args.length - 1]);
            File[] files = file.listFiles();
            paths = new ArrayList<File>();
            for (File f : files) {
                if (f.getAbsolutePath().endsWith(".neci")) {
                    paths.add(f);
                }
            }
        }

        @Override
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            double result = 0.00;
            double sum = 0.00;
            int count = 0;
            for (File path : paths) {
                System.out.println("*************************neciFile" + path.getName());
                LOG.info("*************************neciFile" + path.getName());
                FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(path, filters);
                reader.createSchema(schema);
                reader.filterNoCasc();
                reader.createFilterRead();
                while (reader.hasNext()) {
                    Record r = reader.next();
                    if (r.get(0).toString().startsWith("PROMO")) {
                        List<Record> psL = (List<Record>) r.get(1);
                        for (Record ps : psL) {
                            List<Record> lL = (List<Record>) ps.get(0);
                            count += lL.size();
                            for (Record l : lL) {
                                double res = (float) l.get(0) * (1 - (float) l.get(1));
                                sum += res;
                                result += res;
                            }
                        }
                    } else {
                        List<Record> psL = (List<Record>) r.get(1);
                        for (Record ps : psL) {
                            List<Record> lL = (List<Record>) ps.get(0);
                            count += lL.size();
                            for (Record l : lL) {
                                double res = (float) l.get(0) * (1 - (float) l.get(1));
                                sum += res;
                            }
                        }
                    }
                }
                reader.close();
            }
            context.write(NullWritable.get(), new Text(count + "|" + result + "|" + sum));
        }
    }

    public static class myReduce extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double result = 0.00;
            double sum = 0.00;
            int no = 0;
            for (Text value : values) {
                String[] tmp = value.toString().split("\\|");
                result += Double.parseDouble(tmp[1]);
                sum += Double.parseDouble(tmp[2]);
                no += Integer.parseInt(tmp[0]);
            }
            result = result / sum * 100;
            java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
            nf.setGroupingUsed(false);
            context.write(NullWritable.get(), new Text(no + " | " + nf.format(result)));
        }
    }

    public static class myCombiner extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double result = 0.00;
            double sum = 0.00;
            int no = 0;
            for (Text value : values) {
                String[] tmp = value.toString().split("\\|");
                result += Double.parseDouble(tmp[1]);
                sum += Double.parseDouble(tmp[2]);
                no += Integer.parseInt(tmp[0]);
            }
            context.write(key, new Text(no + "|" + result + "|" + sum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("args", args);
        int pathNum = Integer.parseInt(args[0]);
        Schema inputSchema = new Schema.Parser().parse(new File(args[1]));
        String result = args[2];
        int i = 5;

        Job job = new Job(conf, "Q14MapJobLocal");
        job.setJarByClass(Q14MapJobLocal.class);

        AvroJob.setInputKeySchema(job, inputSchema);

        job.setMapperClass(myMap.class);
        job.setReducerClass(myReduce.class);
        //        job.setCombinerClass(myCombiner.class);

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
        int res = ToolRunner.run(new Configuration(), new Q14MapJobLocal(), args);
        System.exit(res);
    }
}
