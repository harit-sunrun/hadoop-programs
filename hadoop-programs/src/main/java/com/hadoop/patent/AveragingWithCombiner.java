package com.hadoop.patent;

/**
 * User: hhimanshu
 * Date: 7/31/12
 * Time: 2:18 PM
 *
 * @author Harit Himanshu</a>
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;


public class AveragingWithCombiner extends Configured implements Tool {

    public static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        static enum ClaimsCounters { MISSING, QUOTED };

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {

            String fields[] = value.toString().split(",", -20);
            String country = fields[4];
            String numClaims = fields[8];

            if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {
                output.collect(new Text(country), new Text(numClaims + ",1"));
            }
        }
    }

    public static class Combine extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {

            double sum = 0;
            int count = 0;
            while (values.hasNext()) {
                String fields[] = values.next().toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            output.collect(key, new Text(sum + "," + count));
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, DoubleWritable> output,
                           Reporter reporter) throws IOException {

            double sum = 0;
            int count = 0;
            while (values.hasNext()) {
                String fields[] = values.next().toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            output.collect(key, new DoubleWritable(sum/count));
        }
    }

    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a JobConf using the processed conf
        JobConf job = new JobConf(conf, AveragingWithCombiner.class);

        // Process custom command-line options
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Specify various job-specific parameters
        job.setJobName("AveragingWithCombiner");
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Submit the job, then poll for progress until the job is complete
        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new Configuration(), new AveragingWithCombiner(), args);

        System.exit(res);
    }
}
