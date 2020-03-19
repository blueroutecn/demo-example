//package com.thirdlucky.hbase.mr;
//
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//
//import java.io.IOException;
//
//public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
//    @Override
//    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        values.forEach(v -> {
//            String[] fileds = v.toString().split("\t");
//            Put put = new Put(Bytes.toBytes(fileds[0]));
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fileds[1]));
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fileds[2]));
//            try {
//                context.write(NullWritable.get(), put);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//    }
//}
