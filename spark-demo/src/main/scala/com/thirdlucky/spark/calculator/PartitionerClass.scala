package com.thirdlucky.spark.calculator

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionerClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD((1 to 6).zip('a' to 'z'))
    list.partitionBy(MyPartitioner(3)).saveAsTextFile("out")
  }
}

object MyPartitioner {
  def apply(numPartition: Int): MyPartitioner = new MyPartitioner(numPartition)
  class MyPartitioner(numPartition: Int) extends Partitioner {
    override def numPartitions: Int = numPartition
    override def getPartition(key: Any): Int = 1
  }
}
