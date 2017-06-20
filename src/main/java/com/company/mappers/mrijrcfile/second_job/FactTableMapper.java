package com.company.mappers.mrijrcfile.second_job;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class FactTableMapper extends Mapper<Object, BytesRefArrayWritable, NullWritable, Text> {


    @Override
    protected void map(Object key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
        List<Map<Long, List<String>>> cache = new ArrayList<>();
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());

        RCFile.Reader reader = new RCFile.Reader(fs, paths[0], context.getConfiguration());
        final LongWritable readRows = new LongWritable(1);
        final BytesRefArrayWritable cols = new BytesRefArrayWritable();
        ColumnarSerDe serDe = null;
        try {
            serDe = new ColumnarSerDe();
            while (reader.next(readRows)) {
                List<String> fields = new ArrayList<>();
                reader.getCurrentRow(cols);
                final ColumnarStruct row = (ColumnarStruct) serDe.deserialize(cols);
                final ArrayList<Object> objects = row.getFieldsAsList();
                for (final Object object : objects) {
                    fields.add(String.valueOf(object));
                }
                String keyDimenstion = fields.get(0);
                fields.remove(0);
                Map<Long, List<String>> cacheElement = new HashMap<>();
                cacheElement.put(Long.valueOf(keyDimenstion), fields);
                cache.add(cacheElement);
            }
        } catch (SerDeException e) {
            e.printStackTrace();
        }

        final Properties tbl = new Properties();
        tbl.setProperty("columns", "firsttablekey,count,");
        tbl.setProperty("columns.types", "string:string");

        ColumnarSerDe serDe1 = null;

        BytesRefArrayWritable result = new BytesRefArrayWritable();
        try {
            serDe1 = new ColumnarSerDe();
            serDe1.initialize(context.getConfiguration(), tbl);
            final ColumnarStruct row = (ColumnarStruct) serDe1.deserialize(value);
            final ArrayList<Object> objects = row.getFieldsAsList();
            int index = 0;
            List<String> factTableFields = new ArrayList<>();
            for (final Object object : objects) {
                // Lazy decompression happens here
                String currentFieldValue = String.valueOf(object);
                factTableFields.add(currentFieldValue);
            }

            Long factTableForeignKey = Long.valueOf(factTableFields.get(0));
            factTableFields.remove(0);

            Map<Long, List<String>> firstTable = cache.get(0);
            if (firstTable.containsKey(factTableForeignKey)) {
                List<String> firstTableFields = firstTable.get(factTableForeignKey);
                firstTableFields.addAll(factTableFields);
                context.write(NullWritable.get(), new Text(String.join(" ", firstTableFields)));
            }


        } catch (SerDeException e) {
            e.printStackTrace();
        }



    }
}
