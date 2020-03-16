package com.thirdlucky.sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object WindowSlidingObject {
  def main(args: Array[String]): Unit = {
//    scala 滑动窗口
//    val iterator: Iterator[List[Int]] = List(1, 2, 3, 4, 5, 6, 7, 8, 9).sliding(3, 2)
//    iterator.foreach(iter=>println(iter.mkString(",")))
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setAppName("spark-stream-window").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop-node1:9092,hadoop-node2:9092,hadoop-node3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("spark-topic")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    val windowStream = stream.map(_.value).window(Seconds(9), Seconds(3))

    val wordStream: DStream[String] = windowStream.flatMap(_.split(" "))
    val tupleStream: DStream[(String, Int)] = wordStream.map((_, 1))

    val sumStream: DStream[(String, Int)] = tupleStream.reduceByKey(_ + _)

    sumStream.print()

    // 启动采集器
    streamingContext.start()
    // Driver 等待采集器执行
    streamingContext.awaitTermination()
  }
}
