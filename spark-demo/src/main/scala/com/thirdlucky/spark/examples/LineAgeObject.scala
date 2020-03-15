package com.thirdlucky.spark.examples

import org.apache.spark.rdd.RDD

object LineAgeObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List(1 to 6))

    val map: RDD[(Range.Inclusive, Int)] = list.map((_, 1))
    val reduceRDD: RDD[(Range.Inclusive, Int)] = map.reduceByKey(_ + _)

    println(reduceRDD.collect())
  }
}
