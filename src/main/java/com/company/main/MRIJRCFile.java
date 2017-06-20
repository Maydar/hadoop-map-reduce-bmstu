package com.company.main;

import com.company.mappers.mrijrcfile.first_job.FirstTableMapper;
import com.company.mappers.mrijrcfile.first_job.SecondTableMapper;
import com.company.mappers.mrijrcfile.first_job.ThirdTableMapper;
import com.company.mappers.mrijrcfile.second_job.FactTableMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceOutputFormat;

public class MRIJRCFile extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "firstJob");
        job.setJarByClass(MRIJRCFile.class);

        Path firstTablePath = new Path(args[0]);
        Path secondTablePath = new Path(args[1]);
        Path factTablePath = new Path(args[2]);
        Path outFirstJobPath = new Path(args[3]);
        Path outSecondJobPath = new Path(args[4]);

        MultipleInputs.addInputPath(job, firstTablePath, RCFileMapReduceInputFormat.class, FirstTableMapper.class);
        MultipleInputs.addInputPath(job, secondTablePath, RCFileMapReduceInputFormat.class, SecondTableMapper.class);
        MultipleInputs.addInputPath(job, secondTablePath, RCFileMapReduceInputFormat.class, ThirdTableMapper.class);

        job.setOutputFormatClass(RCFileMapReduceOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //FileOutputFormat.setOutputPath(job, outFirstJobPath);
        RCFileMapReduceOutputFormat.setOutputPath(job, outFirstJobPath);
        //RCFileMapReduceOutputFormat.setColumnNumber(conf, 3);
//        RCFileMapReduceOutputFormat.setCompressOutput(job, true);

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);

        DistributedCache.addCacheFile(outFirstJobPath.toUri(), conf);

        Job secondJob = new Job(conf, "secondJob");
        secondJob.setInputFormatClass(RCFileMapReduceInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(secondJob, factTablePath);
        FileOutputFormat.setOutputPath(secondJob, outSecondJobPath);
        secondJob.setJarByClass(MRIJ.class);
        secondJob.setOutputKeyClass(NullWritable.class);
        secondJob.setOutputValueClass(Text.class);
        secondJob.setMapperClass(FactTableMapper.class);
        secondJob.setNumReduceTasks(0);
        secondJob.waitForCompletion(true);

        return 0;
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRIJRCFile(), args));
    }
}
