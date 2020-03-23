package com.thirdlucky.flinkdemo.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object KafkaSource {
  def main(args: Array[String]): Unit = {
    def getProps = {
      val props = new Properties
      props.put("bootstrap.servers", "hadoop-node1:9092")
      props.put("group.id", "consumer-group")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("auto.offset.reset", "latest")
      props
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props: Properties = getProps
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema, props))

    stream.print()

    env.execute("kafka-source")
  }
}
