package com.company.mappers.second_job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author maydar
 *
 */
public class ResultReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count = 0;
        List<String> result = new ArrayList<String>();
        for (Text value : values) {
            count++;
            result.add(value.toString());
        }

        if (count == 3) {
            System.out.println(key.toString() + ": " + Arrays.toString(result.toArray()));
        }
    }

}
