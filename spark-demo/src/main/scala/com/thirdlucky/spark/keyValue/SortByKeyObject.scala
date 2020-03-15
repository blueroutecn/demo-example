package com.thirdlucky.spark.keyValue

import org.apache.spark.{SparkConf, SparkContext}

object SortByKeyObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-orderByKey").setMaster("local[*]")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(Array((User("shne",33),20000),(User("zhang",18),20000),(User("yan",3),25000),(User("yang",55),29000),(User("xue",50),20000)))

    rdd.sortByKey(true).collect().foreach(println)
  }
}
object User{
  def apply(name: String, age: Int): User = new User(name, age)
  class User(var name: String, var age: Int) extends Serializable with Ordered[User]  {
    override def compare(that: User): Int = {
      this.age - that.age
    }
    override def toString: String = "(" + name + "," + age + ")"
  }
}


