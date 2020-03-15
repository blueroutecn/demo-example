package com.thirdlucky.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaCountStreamingObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setAppName("spark-stream-test").setMaster("local[*]")
    val streaming = new StreamingContext(conf,Seconds(3))

    val receiver: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streaming,
      "192.268.227.135:2181", // zookeeper 的 host 和 port
      "spark-group", // 消息分组
      Map("spark-topic" -> 3)
    )

    val wordRDD: DStream[String] = receiver.flatMap(_._2.split(" "))
    val tupleRDD: DStream[(String, Int)] = wordRDD.map((_, 1))
    val wordCount: DStream[(String, Int)] = tupleRDD.reduceByKey(_ + _)

    wordCount.print()
    //TODO: 需要先学习 zookeeper kafka flume

    // 启动采集器
    streaming.start()
    // Driver 等待采集器执行
    streaming.awaitTermination()
  }
}
