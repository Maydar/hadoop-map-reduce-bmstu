package com.company.main;

import com.company.mappers.mrij.first_job.FirstTableMapper;
import com.company.mappers.mrij.first_job.SecondTableMapper;
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

        MultipleInputs.addInputPath(job, firstTablePath, TextInputFormat.class, FirstTableMapper.class);
        MultipleInputs.addInputPath(job, secondTablePath, TextInputFormat.class, SecondTableMapper.class);

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
//        Scan scan_customer = new Scan();
//        scan_customer.setAttribute("scan.attributes.table.name",
//                Bytes.toBytes("customer"));
//
//        Scan scan_car = new Scan();
//        scan_car.setAttribute("scan.attributes.table.name", Bytes.toBytes("car"));
//
//
//        Configuration config = HBaseConfiguration.create();
//        ControlledJob controlledJob1 = new ControlledJob(config);
//        controlledJob1.setJobName("Select customer");
//
//        ControlledJob controlledJob2 = new ControlledJob(config);
//        controlledJob2.setJobName("Select car");
//
//        ControlledJob controlledJob3 = new ControlledJob(config);
//
//        Job job = controlledJob1.getJob();
//        Job job1 = controlledJob2.getJob();
//
//        Job job3 = controlledJob3.getJob();
//
//        TableMapReduceUtil.initTableMapperJob(
//                "customer",
//                scan_customer,
//                SelectCustomerMapper.class,
//                IntWritable.class,
//                Text.class,
//                job
//        );
//
//        TableMapReduceUtil.initTableMapperJob(
//                "car",
//                scan_car,
//                SelectCarMapper.class,
//                IntWritable.class,
//                Text.class,
//                job1
//        );
//
//        /*FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
//*/
//        job.setJarByClass(MRIJ.class);
//        job1.setJarByClass(MRIJ.class);
//
//        DistributedCache.addCacheFile(new Path(args[3]).toUri(), job.getConfiguration());
//        DistributedCache.addCacheFile(new Path(args[4]).toUri(), job1.getConfiguration());
//        DistributedCache.addCacheFile(new Path(args[4]).toUri(), job3.getConfiguration());
//
//        JobControl control = new JobControl("MRIJ");
//        control.addJob(controlledJob1);
//        control.addJob(controlledJob2);
//        control.addJob(controlledJob3);
//        control.run();

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRIJ(), args));
    }
}
