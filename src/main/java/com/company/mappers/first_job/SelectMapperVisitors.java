package com.company.mappers.first_job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import java.io.IOException;


public class SelectMapperVisitors extends Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {
    @Override
    protected void map(WritableComparable key, HCatRecord value, org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        if ((Integer) value.get(3) < 24) {
            context.write(new IntWritable((Integer) value.get(1)), new IntWritable((Integer) value.get(3)));
        }
    }
}
