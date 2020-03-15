package com.thirdlucky.spark.shareData

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object BroadcastObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-broadcast").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val map = Map((1, 1), (2, 2), (3, 3))

    // 如果用 join 会做笛卡尔积
    val broadcast = context.broadcast(map)

    val rdd = list.map {
      case (k, v) => {
        if(map.contains(k)){
          (k, (v, map.get(k).get))
        }
      }
    }

    rdd.collect().foreach(println)
  }
}
