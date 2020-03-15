package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test1").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(1 to 16,4)

    val glomRDD: RDD[Array[Int]] = list.glom()
    glomRDD.collect.foreach(x=> println(x.mkString(",")))

    val maxRDD: RDD[Int] = glomRDD.map(_.max)
    maxRDD.collect().foreach(println)
  }
}
