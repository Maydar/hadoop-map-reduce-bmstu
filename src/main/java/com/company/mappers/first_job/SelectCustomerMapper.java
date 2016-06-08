package com.company.mappers.first_job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author maydar
 *
 */
public class SelectCustomerMapper extends TableMapper<IntWritable, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String age = Arrays.toString(value.getValue("common".getBytes(), "age".getBytes()));
        String name = Arrays.toString(value.getValue("personal".getBytes(), "name".getBytes()));
        if (Integer.valueOf(age) < 45) {
            context.write(new IntWritable(Integer.valueOf(key.toString())),
                    new Text(name));
        }
    }
}
