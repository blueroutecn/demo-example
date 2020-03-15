package com.thirdlucky.spark.actions

import org.apache.spark.rdd.RDD

object CountByKey {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list1 = context.makeRDD(List(1, 2, 5, 4, 123, 12, 45, 65, 6, 7, 23, 2, 46, 6, 3, 345, 5, 6, 3, 4))
    val list2: RDD[(String, Int)] = context.makeRDD(List(("a", 1), ("b", 1), ("c", 1), ("a", 1), ("a", 1)))

    list1.countByValue().foreach(println)
    println(list2.countByKey().mkString(","))
  }
}
