package com.company.mappers.first_job;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author maydar
 * @since 08.06.16
 */
public class SelectCarMapper extends TableMapper<IntWritable, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String model = Arrays.toString(value.getValue("common".getBytes(), "model_name".getBytes()));
        context.write(new IntWritable(Integer.valueOf(key.toString())),
                new Text(model));
    }
}
