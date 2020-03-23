package com.thirdlucky.flinkdemo.sink

import java.util.Properties

import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
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
    val sensorStream: DataStream[SensorReading] = stream.map(new SensorMapper)

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.227.161")
      .setPort(6379)
      .build()

    sensorStream.addSink(new RedisSink(config,new SensorRedisMapper ))
    stream.print()

    env.execute("sink-test")
  }
}

class SensorMapper extends RichMapFunction[String,SensorReading]{
  override def map(in: String): SensorReading = {
    val words = in.split(",")
    SensorReading(words(0),words(1).toLong,words(2).toDouble)
  }
}

class SensorRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"senor_record")
  }

  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
