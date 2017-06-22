package com.company.mappers.mrij.first_job;


import com.company.utils.Clause;
import com.company.utils.Keys;
import com.company.utils.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class CustomerTableMapper extends Mapper<Object, Text, LongWritable, Text> {

    private List<Map<String, String>> selectFields = new ArrayList<>();
    private List<Map<String, String>> tables = new ArrayList<>();
    private List<Map<String, String>> restrictions = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        JSONParser parser = new JSONParser();
        FileReader fileReader = new FileReader(paths[0].toString());
        try {

            Object obj = parser.parse(fileReader);
            JSONObject jsonObject = (JSONObject) obj;

            JSONArray selectFields = (JSONArray) jsonObject.get(Keys.SELECT);
            Iterator<JSONObject> selectFieldsIterator = selectFields.iterator();

            while (selectFieldsIterator.hasNext()) {
                Map<String, String> selectField = new HashMap<>();
                JSONObject item = selectFieldsIterator.next();
                selectField.put(Keys.FIELD, (String) item.get(Keys.FIELD));
                selectField.put(Keys.TABLE, (String) item.get(Keys.TABLE));
                this.selectFields.add(selectField);
            }

            JSONArray tables = (JSONArray) jsonObject.get(Keys.TABLES);
            Iterator<JSONObject> tablesIterator = tables.iterator();
            while (tablesIterator.hasNext()) {
                Map<String, String> table = new HashMap<>();

                JSONObject item = tablesIterator.next();
                String tableName = (String) item.get(Keys.NAME);
                String tableType = (String) item.get(Keys.TYPE);
                String tableFields = (String) item.get(Keys.FIELDS);
                table.put(Keys.NAME, tableName);
                table.put(Keys.TYPE, tableType);
                table.put(Keys.FIELDS, tableFields);
                this.tables.add(table);
            }

            JSONArray restrictions = (JSONArray) jsonObject.get(Keys.RESTRICTIONS);
            Iterator<JSONObject> restrictionsIterator = restrictions.iterator();
            while (restrictionsIterator.hasNext()) {
                Map<String, String> restriction = new HashMap<>();

                JSONObject item = restrictionsIterator.next();
                String restrictionField = (String) item.get(Keys.FIELD);
                String restrictionTable = (String) item.get(Keys.TABLE);
                String restrictionValue = (String) item.get(Keys.VALUE);
                String restrictionOperation = (String) item.get(Keys.OPERATION);
                restriction.put(Keys.FIELD, restrictionField);
                restriction.put(Keys.TABLE, restrictionTable);
                restriction.put(Keys.VALUE, restrictionValue);
                restriction.put(Keys.OPERATION, restrictionOperation);
                this.restrictions.add(restriction);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
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
