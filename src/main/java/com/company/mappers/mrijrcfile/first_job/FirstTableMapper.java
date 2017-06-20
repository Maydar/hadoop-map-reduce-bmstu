package com.company.mappers.mrijrcfile.first_job;


import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.TypeInfo;

import java.io.IOException;
import java.util.*;


public class FirstTableMapper extends Mapper<LongWritable, BytesRefArrayWritable, NullWritable, BytesRefArrayWritable> {

    private int numCols = 3;

    private BytesRefArrayWritable bytes;
    List<String> fields = new ArrayList<>();
    List<String> allFields = Arrays.asList("key", "region", "city");
    List<String> selectFields = Arrays.asList("region", "city");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        bytes = new BytesRefArrayWritable(numCols);
    }

    @Override
    protected void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
        bytes.clear();
        final Properties tbl = new Properties();
        tbl.setProperty("columns", "key,region,country");
        tbl.setProperty("columns.types", "string:string:string");

        ColumnarSerDe serDe = null;
        StringBuilder builder = new StringBuilder();
        BytesRefArrayWritable tmpresult = new BytesRefArrayWritable();
        BytesRefArrayWritable result = new BytesRefArrayWritable();
        try {
            serDe = new ColumnarSerDe();
            serDe.initialize(context.getConfiguration(), tbl);
            final ColumnarStruct row = (ColumnarStruct) serDe.deserialize(value);
            final ArrayList<Object> objects = row.getFieldsAsList();
            int index = 0;
            for (final Object object : objects) {
                // Lazy decompression happens here
                String currentFieldValue = String.valueOf(object);
                fields.add(currentFieldValue);
                byte[] fieldData = currentFieldValue.getBytes("UTF-8");
                BytesRefWritable cu = null;

                cu = new BytesRefWritable(fieldData, 0, fieldData.length);
                tmpresult.set(index, cu);

                index++;
            }
            Boolean firstClause = false;
            Boolean secondClause = false;

            int rsIndex = 0;
            for (String selectField: selectFields) {
                String targetField = fields.get(allFields.indexOf(selectField));
                if (selectField.equals("region") && targetField.equals("asia") ) {
                    firstClause = true;
                    result.set(rsIndex, tmpresult.get(rsIndex));
                }
                if (selectField.equals("city") && targetField.equals("japan") ) {
                    secondClause = true;
                    result.set(rsIndex, tmpresult.get(rsIndex));
                }
                rsIndex++;
            }

            if (firstClause && secondClause) {
                context.write(NullWritable.get(), result);
            }
        } catch (SerDeException e) {
            e.printStackTrace();
        }


//        Text result = new Text(builder.toString());


//        String[] cols = value.toString().split("\\|");
//
//        for (int i = 0; i < numCols; i++){
//
//            byte[] fieldData = cols[i].getBytes("UTF-8");
//
//            BytesRefWritable cu = null;
//
//            cu = new BytesRefWritable(fieldData, 0, fieldData.length);
//
//            bytes.set(i, cu);
//
//        }


        //Configuration conf = context.getConfiguration();
        //Clause clause = new Clause("region", Operation.EQUAL, "Asia");
//        List<String> fields = new ArrayList<>();
//        List<String> allFields = Arrays.asList("key", "region", "city");
//        List<String> selectFields = Arrays.asList("region", "city");


//        StringTokenizer iterator = new StringTokenizer(value.toString());
//
//        while (iterator.hasMoreTokens()) {
//            String field = iterator.nextToken();
//            fields.add(field);
//        }
//
//        String tableKey = fields.get(0);
//        List<String> result = new ArrayList<>();
//
//        Boolean firstClause = false;
//        Boolean secondClause = false;
//
//        for (String selectField: selectFields) {
//            String targetField = fields.get(allFields.indexOf(selectField));
//            if (selectField.equals("region") && targetField.equals("asia") ) {
//                firstClause = true;
//                result.add(targetField);
//            }
//            if (selectField.equals("city") && targetField.equals("japan") ) {
//                secondClause = true;
//                result.add(targetField);
//            }
//        }
//
//        result.add(fields.get(fields.size() - 1));
//
//        if (firstClause && secondClause) {
//            context.write(new LongWritable(Long.valueOf(tableKey)), new Text(String.join(",", result)));
//        }

    }
}
