package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(1 to 16,4)

    println("之前"+list.partitions.size)
    val coalRDD: RDD[Int] = list.coalesce(3)
    println("之后"+coalRDD.partitions.size)
    coalRDD.saveAsTextFile("out")
  }
}
