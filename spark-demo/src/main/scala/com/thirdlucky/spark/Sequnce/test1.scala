package com.thirdlucky.spark.Sequnce

import org.apache.spark.rdd.RDD

object test1 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List("hadoop", "spark", "hive", "istio"))

    val search = new Search("h")

    val match1: RDD[String] = search.getMatch2(rdd)
    // 调用的 isMatch方法，是成员函数，所以需要对象序列化
    //    val match1: RDD[String] = search.getMatch1(rdd)
    match1.collect().foreach(println)
  }
}

class Search(query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    // 局部变量是一个字符串，可以序列化，把字符串传过去和对象无关
    var q = query
    rdd.filter(x => x.contains(q))
    // 会报错，query 是对象成员属性，所以需要对象序列化
    //  rdd.filter(x => x.contains(query))
  }
}