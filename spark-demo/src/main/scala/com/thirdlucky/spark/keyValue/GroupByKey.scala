package com.thirdlucky.spark.keyValue

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-groupByKey").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(Array("one", "two", "three", "four", "five", "six", "seven", "one", "two", "three", "three"))
    list.map((_, 1)).groupByKey().collect().foreach(println)
    //word count
    list.map((_, 1)).groupByKey().map(t => (t._1, t._2.sum)).collect().foreach(println)
  }
}
