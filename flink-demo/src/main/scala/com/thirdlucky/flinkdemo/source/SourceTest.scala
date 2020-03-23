package com.thirdlucky.flinkdemo.source

import org.apache.flink.streaming.api.scala._
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 列表中读取
    val stream = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      )
    )

    //    // 文件中读取
    //    val stream: DataStream[String] = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\sensor.txt")


    // 从元素
    //    env.fromElements(1,2,"name","male",false).print()
    stream.print("source1")
    env.execute("test-job")
  }
}
