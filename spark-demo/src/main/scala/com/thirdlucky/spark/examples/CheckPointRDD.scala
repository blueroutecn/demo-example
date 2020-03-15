package com.thirdlucky.spark.examples

import org.apache.spark.rdd.RDD

object CheckPointRDD {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setCheckpointDir("check")

    val list = context.makeRDD(List(1 to 3))
    val mapRDD = list.map((_, 1))

    val reduceRDD1 = mapRDD.reduceByKey(_ + _)
    reduceRDD1.foreach(println)
    println(reduceRDD1.toDebugString)

    val reduceRDD2 = mapRDD.reduceByKey(_ + _)
    reduceRDD2.checkpoint()
    reduceRDD2.foreach(println)
    println(reduceRDD2.toDebugString)
  }
}
