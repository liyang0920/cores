package local.cores.query.q19;

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

public class Q19MapJobLocal extends Configured implements Tool {
    private final static Log LOG = LogFactory.getLog(Q19MapJobLocal.class);

    public static class myMap extends Mapper<Text, NullWritable, NullWritable, Text> {
        private FilterOperator[] filters;
        private Schema schema;
        private List<File> paths;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            schema = AvroJob.getInputKeySchema(context.getConfiguration());
            String[] args = conf.getStrings("args");
            String brandNo = args[3];
            filters = new FilterOperator[6];
            int p_container = Integer.parseInt(args[4]);
            String[] com_p = new String[p_container];
            int i = 5;
            for (int m = 0; m < p_container; m++) {
                com_p[m] = args[i + m];
            }
            i += p_container;
            int p_size = Integer.parseInt(args[i++]);
            float f1 = Float.parseFloat(args[i++]);
            float f2 = Float.parseFloat(args[i++]);
            int l_shipmode = Integer.parseInt(args[i++]);
            String[] com_l = new String[l_shipmode];
            for (int m = 0; m < l_shipmode; m++) {
                com_l[m] = args[i + m];
            }
            filters[0] = new Pfilter1(brandNo); //p_brand
            filters[1] = new Pfilter2(com_p); //p_container
            filters[2] = new Pfilter3(p_size); //p_size
            filters[3] = new Lfilter1(f1, f2); //l_quantity
            filters[4] = new Lfilter2(com_l); //l_shipmode
            filters[5] = new Lfilter3(); //l_shipinstruct

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
                    result += (float) r.get(0) * (1 - (float) r.get(1));
                    count++;
                }
                reader.close();
            }
            context.write(NullWritable.get(), new Text(count + "|" + result));
        }
    }

    public static class myReduce extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double result = 0.00;
            int no = 0;
            for (Text value : values) {
                String[] tmp = value.toString().split("\\|");
                no += Integer.parseInt(tmp[0]);
                result += Double.parseDouble(tmp[1]);
            }
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
            int no = 0;
            for (Text value : values) {
                String[] tmp = value.toString().split("\\|");
                no += Integer.parseInt(tmp[0]);
                result += Double.parseDouble(tmp[1]);
            }
            context.write(key, new Text(no + "|" + result));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("args", args);
        int pathNum = Integer.parseInt(args[0]);
        Schema inputSchema = new Schema.Parser().parse(new File(args[1]));
        String result = args[2];
        int i = 5;
        i += Integer.parseInt(args[4]);
        i += 3;
        i += Integer.parseInt(args[i]);
        ++i;

        Job job = new Job(conf, "Q19MapJobLocal");
        job.setJarByClass(Q19MapJobLocal.class);

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
        int res = ToolRunner.run(new Configuration(), new Q19MapJobLocal(), args);
        System.exit(res);
    }
}
