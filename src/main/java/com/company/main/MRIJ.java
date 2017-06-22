package com.company.main;

import com.company.mappers.mrij.first_job.CustomerTableMapper;
import com.company.mappers.mrij.first_job.DateTableMapper;
import com.company.mappers.mrij.second_job.FactTableMapper;
import com.company.mappers.mrij.second_job.ResultReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MRIJ extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "firstJob");
        job.setJarByClass(MRIJ.class);

        Path firstTablePath = new Path(args[0]);
        Path secondTablePath = new Path(args[1]);
        Path factTablePath = new Path(args[2]);
        Path outFirstJobPath = new Path(args[3]);
        Path outSecondJobPath = new Path(args[4]);
        Path queryPath = new Path(args[5]);

        DistributedCache.addCacheFile(queryPath.toUri(), job.getConfiguration());

        MultipleInputs.addInputPath(job, firstTablePath, TextInputFormat.class, CustomerTableMapper.class);
        MultipleInputs.addInputPath(job, secondTablePath, TextInputFormat.class, DateTableMapper.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outFirstJobPath);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);


        DistributedCache.addCacheFile(outFirstJobPath.toUri(), conf);

        Job secondJob = new Job(conf, "secondJob");

        secondJob.setInputFormatClass(TextInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(secondJob, factTablePath);
        FileOutputFormat.setOutputPath(secondJob, outSecondJobPath);


        secondJob.setJarByClass(MRIJ.class);
        secondJob.setOutputKeyClass(LongWritable.class);
        secondJob.setOutputValueClass(Text.class);
        secondJob.setMapperClass(FactTableMapper.class);

        secondJob.setReducerClass(ResultReducer.class);
        secondJob.waitForCompletion(true);



        return 0;

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRIJ(), args));
    }
}
