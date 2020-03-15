package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val context = new SparkContext(sparkConf)

    val lines: RDD[String] = context.textFile("hdfs://hadoop-node1:9000/data/spark/test/word.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val map: RDD[(String, Int)] = words.map((_, 1))
    val wordSum: RDD[(String, Int)] = map.reduceByKey(_ + _)
    val result: Array[(String, Int)] = wordSum.collect()

    result.foreach(println)
  }
}
