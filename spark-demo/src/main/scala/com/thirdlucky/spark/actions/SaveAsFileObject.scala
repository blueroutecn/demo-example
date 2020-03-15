package com.thirdlucky.spark.actions

import org.apache.spark.rdd.RDD

object SaveAsFileObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-save").setMaster("local[*]")
    val context = new SparkContext(conf)
    //    val list = context.makeRDD(List(User("zhangsan",12),User("lisi",15),User("wangwu",20)),2)
    val list: RDD[(String, Int)] = context.makeRDD(List(("zhangsan", 1), ("lisi", 2), ("wangwu", 3)),2)
    list.saveAsTextFile("out/save/text")
    list.saveAsObjectFile("out/save/obj")
    list.saveAsSequenceFile("out/save/seq")
  }
}

object User {
  def apply(name: String, age: Int): User = new User(name, age)
}

class User(var name: String, var age: Int) extends Serializable {
}

