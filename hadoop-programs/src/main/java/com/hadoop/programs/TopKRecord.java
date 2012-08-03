package com.hadoop.programs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * User: hhimanshu
 * Date: 8/1/12
 * Time: 5:42 AM
 *
 * @author Harit Himanshu</a>
 */
public class TopKRecord extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // your map code goes here
            String[] fields = value.toString().split(",");
            Text year = new Text("topKRecords");
            LongWritable claims = new LongWritable();

            if (fields[8].length() > 0 && (!fields[8].startsWith("\""))) {
                claims.set(Long.parseLong(fields[8].toString()));
                context.write(year, claims);
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // your reduce function goes here
            TreeSet<Long> uniqueSorted = new TreeSet<Long>();
            for (LongWritable value: values) {
                uniqueSorted.add(Long.parseLong(value.toString()));
            }

            StringBuffer sb = new StringBuffer();
            Iterator reverseIterator = uniqueSorted.descendingIterator();
            Configuration conf = context.getConfiguration();
            int maxRecords = Integer.parseInt(conf.get("NumberOfRecords"));
            int count = 0;
            while (reverseIterator.hasNext()) {
                    if (count == maxRecords) {
                        break;
                    }
                    sb.append(reverseIterator.next().toString() + ",");
                    count ++;
                }
            context.write(key, new Text(sb.toString()));
        }
    }

    public int run(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.set("NumberOfRecords", args[2]);

        Job job = new Job(conf);
        job.setJarByClass(TopKRecord.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setJobName("TopKRecord");

//        job.setNumReduceTasks(0); // to just run Mapper
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int ret = ToolRunner.run(new TopKRecord(), args);
        System.exit(ret);
    }
}
