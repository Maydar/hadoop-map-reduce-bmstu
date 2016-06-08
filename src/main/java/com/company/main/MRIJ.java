package com.company.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.ql.io.ORCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by maydar on 27.12.15.
 */
public class MRIJ extends Configured implements Tool {


    public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                context.write(new Text(nextToken), new IntWritable(1));
            }
        }
    }


    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();

            }

            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {

        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config,
                "MRIJ");

        job.setInputFormatClass(RCFileMapReduceInputFormat.class);


        /*

                Configuration conf = new Configuration();

        Job job = Job.getInstance(,

                "wordcount");

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);

        job.setReducerClass(WordCountReduce.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(MRIJ.class);

        return job.waitForCompletion(true) ? 0 : 1;
*/

        job.setJarByClass(MRIJ.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRIJ(), args));

    }
}
