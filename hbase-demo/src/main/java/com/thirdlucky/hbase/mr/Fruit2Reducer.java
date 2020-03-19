//package com.thirdlucky.hbase.mr;
//
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.io.NullWritable;
//
//import java.io.IOException;
//
//public class Fruit2Reducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
//    @Override
//    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
//        values.forEach(put->{
//            try {
//                context.write(NullWritable.get(),put);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//    }
//}
