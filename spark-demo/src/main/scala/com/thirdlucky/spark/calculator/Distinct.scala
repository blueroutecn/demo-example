package com.thirdlucky.spark.calculator

import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-distict").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List(1,2,5,9,12,4,5,1,2,37,12,3,2,5))
    list.distinct.collect.foreach(println)
    list.distinct(2).collect.foreach(println)
  }
}
