package com.thirdlucky.spark.keyValue

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object CombByKeyClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List(("a", 78), ("b", 36), ("a", 26), ("b", 75), ("a", 66)))

    val value = list.combineByKey(
      (_, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + x._2)
    )
    value.map((t) => (t._1, t._2._1.toDouble / t._2._2)).collect().foreach(println)
  }
}
