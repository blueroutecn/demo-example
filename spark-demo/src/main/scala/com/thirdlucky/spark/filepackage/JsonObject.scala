package com.thirdlucky.spark.filepackage

import scala.util.parsing.json.JSON

object JsonObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list: RDD[String] = context.textFile("input/json")

    val json: RDD[Option[Any]] = list.map(JSON.parseFull)
    json.foreach(println)
  }
}
