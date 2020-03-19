//package com.thirdlucky.hbase.mr;
//
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//
//import java.io.IOException;
//
//public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
//    @Override
//    public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
//        Put put = new Put(key.get());
//        for (Cell cell : result.rawCells()) {
//            if ("name".equals(CellUtil.cloneQualifier(cell))) {
//                put.add(cell);
//            }
//        }
//        context.write(key, put);
//    }
//}
