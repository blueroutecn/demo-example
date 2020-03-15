package com.thirdlucky.spark.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-aggregate").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(Array(('a', 1), ('b', 2), ('c', 3), ('a', 5), ('b', 2), ('c', 1), ('a', 7), ('b', 22), ('c', 3)), 2)

    list.glom().collect().foreach(x => println(x.mkString(",")))
    list.aggregateByKey(0)(Math.max(_, _), _ + _).collect().foreach(println)

    // word count
    list.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    list.foldByKey(0)(_ + _).collect().foreach(println)
  }
}
