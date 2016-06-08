package com.company.main;

import com.company.mappers.first_job.SelectCarMapper;
import com.company.mappers.first_job.SelectCustomerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by maydar on 27.12.15.
 */
public class MRIJ extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Scan scan_customer = new Scan();
        scan_customer.setAttribute("scan.attributes.table.name",
                Bytes.toBytes("customer"));

        Scan scan_car = new Scan();
        scan_car.setAttribute("scan.attributes.table.name", Bytes.toBytes("car"));


        Configuration config = HBaseConfiguration.create();
        ControlledJob controlledJob1 = new ControlledJob(config);
        controlledJob1.setJobName("Select customer");

        ControlledJob controlledJob2 = new ControlledJob(config);
        controlledJob2.setJobName("Select car");

        ControlledJob controlledJob3 = new ControlledJob(config);

        Job job = controlledJob1.getJob();
        Job job1 = controlledJob2.getJob();

        Job job3 = controlledJob3.getJob();

        TableMapReduceUtil.initTableMapperJob(
                "customer",
                scan_customer,
                SelectCustomerMapper.class,
                IntWritable.class,
                Text.class,
                job
        );

        TableMapReduceUtil.initTableMapperJob(
                "car",
                scan_car,
                SelectCarMapper.class,
                IntWritable.class,
                Text.class,
                job1
        );

        /*FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
*/
        job.setJarByClass(MRIJ.class);
        job1.setJarByClass(MRIJ.class);

        DistributedCache.addCacheFile(new Path(args[3]).toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(args[4]).toUri(), job1.getConfiguration());
        DistributedCache.addCacheFile(new Path(args[4]).toUri(), job3.getConfiguration());

        JobControl control = new JobControl("MRIJ");
        control.addJob(controlledJob1);
        control.addJob(controlledJob2);
        control.addJob(controlledJob3);
        control.run();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRIJ(), args));

    }
}
