package com.company.mappers.mrij.first_job;


import com.company.utils.Clause;
import com.company.utils.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;


public class FirstTableMapper extends Mapper<Object, Text, LongWritable, Text> {




    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
        //Clause clause = new Clause("region", Operation.EQUAL, "Asia");
        List<String> fields = new ArrayList<>();
        List<String> allFields = Arrays.asList("key", "region", "city");
        List<String> selectFields = Arrays.asList("region", "city");

        StringTokenizer iterator = new StringTokenizer(value.toString());

        while (iterator.hasMoreTokens()) {
            String field = iterator.nextToken();
            fields.add(field);
        }

        String tableKey = fields.get(0);
        List<String> result = new ArrayList<>();

        Boolean firstClause = false;
        Boolean secondClause = false;

        for (String selectField: selectFields) {
            String targetField = fields.get(allFields.indexOf(selectField));
            if (selectField.equals("region") && targetField.equals("asia") ) {
                firstClause = true;
                result.add(targetField);
            }
            if (selectField.equals("city") && targetField.equals("japan") ) {
                secondClause = true;
                result.add(targetField);
            }
        }

        result.add(fields.get(fields.size() - 1));

        if (firstClause && secondClause) {
            context.write(new LongWritable(Long.valueOf(tableKey)), new Text(String.join(",", result)));
        }

    }
}
