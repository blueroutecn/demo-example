package com.thirdlucky.spark.examples

import org.apache.spark.rdd.RDD

object CacheObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(Array("thirdlucky"))
    val mapRDD: RDD[String] = list.map(_.toString + System.currentTimeMillis())

    mapRDD.collect.foreach(println)
    mapRDD.collect.foreach(println)
    mapRDD.collect.foreach(println)

    val cacheRDD: mapRDD.type = mapRDD.cache()
    cacheRDD.collect.foreach(println)
    cacheRDD.collect.foreach(println)
    cacheRDD.collect.foreach(println)
    println(cacheRDD.toDebugString)

    for (i <- 1 to 1000) {
      Thread.sleep(1000)
      println("wait" + i)
    }
  }
}
