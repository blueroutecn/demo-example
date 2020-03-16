package com.thirdlucky.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object UpdateStateObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setAppName("spark-stream-kafka").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    // 设置检查点目录
    streamingContext.sparkContext.setCheckpointDir("checkpoint")

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

    val wordStream: DStream[String] = stream.flatMap(_.value.split(" "))
    val tupleStream: DStream[(String, Int)] = wordStream.map((_, 1))

    val stateStream: DStream[(String, Int)] = tupleStream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Some(sum)
      }
    }

    stateStream.print()

    // 启动采集器
    streamingContext.start()
    // Driver 等待采集器执行
    streamingContext.awaitTermination()
  }
}
