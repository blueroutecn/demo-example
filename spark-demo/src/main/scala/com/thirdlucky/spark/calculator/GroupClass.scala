package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(1 to 12)

    val groupRDD: RDD[(Int, Iterable[Int])] = list.groupBy(_ % 2)
    groupRDD.collect.foreach(println)

    list.filter(_%2==0).collect.foreach(println)

//    groupRDD.collect.foreach {
//      case (id, datas) => {
//        println(id + "=" + datas.mkString(","))
//      }
//    }
  }
}
