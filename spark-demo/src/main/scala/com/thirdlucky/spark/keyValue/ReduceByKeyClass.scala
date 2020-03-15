package com.thirdlucky.spark.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-groupByKey").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(Array("one", "two", "three", "four", "five", "six", "seven", "one", "two", "three", "three"))
    val listRDD: RDD[(String, Int)] = list.map((_, 1))

    listRDD.reduceByKey(_+_).collect().foreach(println)
  }
}
