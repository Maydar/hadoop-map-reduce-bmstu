package com.company.mappers.mrij.second_job;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class FactTableMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, Map<Long, List<String>>> cache = new HashMap<>();
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        FileStatus[] status = fs.listStatus(paths[0]);

        for (FileStatus statu : status) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(statu.getPath())));
            String line;
            String measureName = "";
            Map<Long, List<String>> valuesMap = new HashMap<>();

            while ((line = br.readLine()) != null) {
                String[] result = line.split("\t");
                String[] values = result[1].split(",");
                valuesMap.put(Long.valueOf(result[0]), Arrays.asList(values));
                measureName = values[values.length - 1];
            }

            cache.put(measureName, valuesMap);
        }


        List<String> fields = new ArrayList<>();
        StringTokenizer iterator = new StringTokenizer(value.toString());
        while (iterator.hasMoreTokens()) {
            String field = iterator.nextToken();
            fields.add(field);
        }

        String measureName = fields.get(0);
        fields.remove(0);

        Map<Long, List<String>> measureCache = cache.get(measureName);


        for (String foreignKey : fields) {
            Long longForeignKey = Long.valueOf(foreignKey);
            if (measureCache.containsKey(longForeignKey)) {
                String values = String.join(" ", measureCache.get(longForeignKey));
                context.write(new LongWritable(fields.indexOf(foreignKey)), new Text(values));
            }
        }

    }
}
