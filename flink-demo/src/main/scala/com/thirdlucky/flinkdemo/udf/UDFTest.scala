package com.thirdlucky.flinkdemo.udf

import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object UDFTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\sensor.txt")

    stream.filter(new StartFilter("sensor_1")).print()

    stream.map(new MyMapper).print()

    env.execute("trans")
  }
}

class StartFilter(var prefix: String) extends  FilterFunction[String]{
  override def filter(t: String): Boolean = t.startsWith(prefix)
}

class MyMapper extends RichMapFunction[String,SensorReading]{
  override def map(in: String): SensorReading = {
    val lines: Array[String] = in.split(",")
    SensorReading(lines(0),lines(1).toLong,lines(2).toDouble)
  }

  override def open(parameters: Configuration): Unit = {
    var subtask = getRuntimeContext.getIndexOfThisSubtask
  }

  override def close(): Unit = {
    // 清理
  }
}

