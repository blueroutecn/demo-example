package com.thirdlucky.spark.calculator

import org.apache.spark.{SparkConf, SparkContext}

object SortClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List(1,5,3,8,4,6,2))

    list.sortBy(x=>x,false).collect.foreach(println)
  }
}
