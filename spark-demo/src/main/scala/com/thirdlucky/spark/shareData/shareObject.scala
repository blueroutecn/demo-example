package com.thirdlucky.spark.shareData

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable.ListBuffer

object shareObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    //    val list = context.makeRDD(List(1, 2, 3, 4), 2)
    //
    //    val accumulator: LongAccumulator = context.longAccumulator("sum")
    //
    //    list.foreach(accumulator.add(_))
    //    println(accumulator.value)

    val textRDD: RDD[String] = context.makeRDD(List("hadoop", "hello", "spark", "hive", "hbase", "world"))
    val accumulator = new ListAccumulator
    context.register(accumulator)

    textRDD.foreach(accumulator.add(_))
    println(accumulator.value)
  }
}

class ListAccumulator extends AccumulatorV2[String, ListBuffer[String]] {
  val list: ListBuffer[String] = new ListBuffer[String]

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[String, ListBuffer[String]] = {
    val newObj = new ListAccumulator()
    newObj.list.appendAll(this.list)
    newObj
  }

  override def reset(): Unit = list.clear

  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.append(v)
    }
  }

  override def merge(other: AccumulatorV2[String, ListBuffer[String]]): Unit = {
    this.list.appendAll(other.value)
  }

  override def value: ListBuffer[String] = list
}
