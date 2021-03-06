//package com.thirdlucky.hbase.mr;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//
//public class Fruit2Driver implements Tool {
//
//    private Configuration conf;
//
//    @Override
//    public int run(String[] args) throws Exception {
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(Fruit2Driver.class);
//
//        TableMapReduceUtil.initTableMapperJob(args[0],new Scan(),Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class,job);
//        TableMapReduceUtil.initTableReducerJob(args[1], Fruit2Reducer.class, job);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//
//        boolean b = job.waitForCompletion(true);
//        return b ? 0 : 1;
//    }
//
//    @Override
//    public void setConf(Configuration configuration) {
//        this.conf = configuration;
//    }
//
//    @Override
//    public Configuration getConf() {
//        return conf;
//    }
//
//    public static void main(String[] args) {
//        Configuration config = new Configuration();
//        try {
//            int run = ToolRunner.run(config, new Fruit2Driver(), args);
//            System.exit(run);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
