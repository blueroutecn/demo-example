package com.thirdlucky.spark.keyValue

import org.apache.spark.{SparkConf, SparkContext}

object MapValueObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-mapValues").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List((1,"a"),(2,"b"),(3,"c")))

    list.mapValues(_+"|||").collect().foreach(println)
  }
}
