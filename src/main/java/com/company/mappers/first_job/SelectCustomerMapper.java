package com.company.mappers.first_job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            FileSystem fs = FileSystem.getLocal(context.getConfiguration());
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(paths[0])));

            bufferedWriter.write(Integer.valueOf(key.toString()));
            bufferedWriter.write(age);
        }
    }
}
