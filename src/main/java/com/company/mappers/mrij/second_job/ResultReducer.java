package com.company.mappers.mrij.second_job;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class ResultReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count = 0;
        List<String> result = new ArrayList<String>();

        for (Text value : values) {
            count++;

            List<String> fields = new LinkedList<>(Arrays.asList(value.toString().split(" ")));
            fields.remove(fields.size() - 1);

            result.add(String.join(" ", fields));
        }

        if (count == 2) {
            context.write(key, new Text(String.join(" ", result)));
        }
    }

}
