package com.company.mappers.first_job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import java.io.IOException;


public class SelectMapperRestaurants extends Mapper<WritableComparable, HCatRecord, IntWritable, Text> {
    @Override
    protected void map(WritableComparable key, HCatRecord value, org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
            context.write(new IntWritable((Integer) value.get(1)), new Text((String) value.get(2)));
    }
}