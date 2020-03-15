package com.thirdlucky.spark.calculator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(1 to 10, 2)

    val i = 4
    i match {
      case 1 => testMap(context)
      case 2 => testMapPartitions(context)
      case 3 => testMapPartitionsWithIndex(context)
      case 4 => testFlatMap(context)
      case _ => println("没有这个指令")
    }

    def testMap(sc: SparkContext): Unit = {
      val mapRDD: RDD[Int] = list.map(_ * 2)
      mapRDD.collect().foreach(println)
    }

    def testMapPartitions(sc: SparkContext): Unit = {
      //    遍历所有的分区
      val map: RDD[Int] = list.mapPartitions(datas => {
        datas.map(_ * 2)
      })
      map.collect().foreach(println)
    }

    def testMapPartitionsWithIndex(sc: SparkContext): Unit = {
      val map: RDD[(String, Int)] = list.mapPartitionsWithIndex {
        case (num, datas) => {
          datas.map(("分区:" + num, _))
        }
      }
      map.collect().foreach(println)
    }

    def testFlatMap(sc: SparkContext) = {
      val array: Array[List[Int]] = Array(List(1, 2), List(3, 4))
      val value: RDD[List[Int]] = sc.makeRDD(array)

      val map: RDD[Int] = value.flatMap(_.toList)
      map.collect().foreach(println)
    }
  }
}
