package com.company.mappers.first_job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author maydar
 * @since 23.05.16
 */
public class SelectMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String age = Arrays.toString(value.getValue("common".getBytes(), "age".getBytes()));
        String name = Arrays.toString(value.getValue("personal".getBytes(), "name".getBytes()));
        if (Integer.valueOf(age) < 45) {
            context.write(new Text(String.valueOf(key)),
                    new Text(name));
        }
    }
}
