package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Collection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)

    val list1 = context.makeRDD(1 to 5)
    val list2 = context.makeRDD('a' to 'e')

    def printRDD(rdd: RDD[Int]): Unit = {
      println(rdd.collect().mkString(","))
    }

//    printRDD(list1.union(list2))
//    printRDD(list1.subtract(list2))
//    printRDD(list1.intersection(list2))
//
//    list1.cartesian(list2).collect.foreach(println)

    list1.zip(list2).collect().foreach(println)
  }
}
