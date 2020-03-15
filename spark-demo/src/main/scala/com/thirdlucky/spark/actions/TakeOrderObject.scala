package com.thirdlucky.spark.actions

object TakeOrderObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-takeOrder").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List(1,5,7,2,3,8))
    list.takeOrdered(3).foreach(println)
    println(list.aggregate(0)(_ + _, _ + _))
  }
}
