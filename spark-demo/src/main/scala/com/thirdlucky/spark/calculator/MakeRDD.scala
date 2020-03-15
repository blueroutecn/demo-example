package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MakeRDD {
  def main(args: Array[String]): Unit = {
//    local[*] 内核数量
    val conf = new SparkConf().setAppName("MakeRDD").setMaster("local[*]")
    val context = new SparkContext(conf)
//    val listRDD: RDD[Int] = context.makeRDD(List(1, 2, 3, 4, 5))
val listRDD: RDD[String] = context.textFile("input",2)
    listRDD.saveAsObjectFile("out")

    //listRDD.collect().foreach(println)


//    读取项目路径 input
//    读取 HDFS    hdfs://hadoop-node1:9000/input
//    读取 file    file:/input
//    val value: RDD[String] = context.textFile("input")
  }
}
