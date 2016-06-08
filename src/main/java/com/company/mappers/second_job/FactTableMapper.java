package com.company.mappers.second_job;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author maydar
 *
 */
public class FactTableMapper extends TableMapper<IntWritable, Text> {

    private Path[] paths;
    private Map<IntWritable, Text> cache = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        FileSystem fs = FileSystem.getLocal(context.getConfiguration());

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(paths[0])));
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] result = line.split(" ");

            cache.put(new IntWritable(Integer.valueOf(result[0])), new Text(result[1]));
        }

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String customerId = Arrays.toString(value.getValue("id".getBytes(), "customer_id".getBytes()));
        if (cache.containsKey(new IntWritable(Integer.valueOf(customerId)))) {
            context.write(new IntWritable(Integer.valueOf(key.toString())),
                    cache.get(new IntWritable(Integer.valueOf(customerId))));
        }
    }
}
