package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-partition").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD((1 to 5).zip('a' to 'f'), 4)

    val partitionRDD: RDD[(Int, Char)] = list.partitionBy(new HashPartitioner(2))
    println("重新分区之前")
    list.glom().collect().foreach(x=>println(x.mkString(",")))
    println("重新分区之后")
    partitionRDD.glom().collect().foreach(x=>println(x.mkString(",")))
  }
}
