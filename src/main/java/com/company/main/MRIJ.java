package com.company.main;

import com.company.mappers.first_job.SelectMapperRestaurants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;


public class MRIJ extends Configured implements Tool {



    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String inputTableName = args[0];
        String outputTableName = args[1];
        String dbName = null;
        //InputJobInfo.create(dbName, inputTableName, null, null);

        ControlledJob controlledJobselectrest = new ControlledJob(conf);
        controlledJobselectrest.setJobName("RestaurantSelectJob");

        ControlledJob controlledJobselectvisitor = new ControlledJob(conf);
        controlledJobselectvisitor.setJobName("VisitorSelectJob");



        Job restaurantselectjob = controlledJobselectrest.getJob();
        HCatInputFormat.setInput(restaurantselectjob, dbName, "restaurantsrc");
        // initialize HCatOutputFormat

        restaurantselectjob.setInputFormatClass(HCatInputFormat.class);
        restaurantselectjob.setJarByClass(MRIJ.class);
        restaurantselectjob.setMapperClass(SelectMapperRestaurants.class);
        restaurantselectjob.setMapOutputKeyClass(IntWritable.class);
        restaurantselectjob.setMapOutputValueClass(IntWritable.class);
        restaurantselectjob.setOutputKeyClass(WritableComparable.class);
        restaurantselectjob.setOutputValueClass(DefaultHCatRecord.class);
        HCatOutputFormat.setOutput(restaurantselectjob, OutputJobInfo.create(dbName, outputTableName, null));
        HCatSchema s = HCatOutputFormat.getTableSchema(conf);
        System.err.println("INFO: output schema explicitly set for writing:"
                + s);
        HCatOutputFormat.setSchema(restaurantselectjob, s);
        restaurantselectjob.setOutputFormatClass(HCatOutputFormat.class);

        Job visitorselectjob = controlledJobselectvisitor.getJob();
        HCatInputFormat.setInput(visitorselectjob, dbName, "restaurantsrc");
        // initialize HCatOutputFormat

        visitorselectjob.setInputFormatClass(HCatInputFormat.class);
        visitorselectjob.setJarByClass(MRIJ.class);
        visitorselectjob.setMapperClass(SelectMapperRestaurants.class);
        visitorselectjob.setMapOutputKeyClass(IntWritable.class);
        visitorselectjob.setMapOutputValueClass(IntWritable.class);
        visitorselectjob.setOutputKeyClass(WritableComparable.class);
        visitorselectjob.setOutputValueClass(DefaultHCatRecord.class);
        HCatOutputFormat.setOutput(visitorselectjob, OutputJobInfo.create(dbName, outputTableName, null));
        HCatSchema s1 = HCatOutputFormat.getTableSchema(conf);
        System.err.println("INFO: output schema explicitly set for writing:"
                + s1);
        HCatOutputFormat.setSchema(visitorselectjob, s1);
        visitorselectjob.setOutputFormatClass(HCatOutputFormat.class);

        JobControl control = new JobControl("MRIJ");
        control.addJob(controlledJobselectrest);
        control.addJob(controlledJobselectvisitor);
        control.run();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRIJ(), args));

    }
}
