package com.thirdlucky.flinkdemo.transfer

import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.streaming.api.scala._

object TransferTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\sensor.txt")

    val srStream = stream.map(line => {
      val words: Array[String] = line.split(",")
      SensorReading(words(0), words(1).toLong, words(2).toDouble)
    })
      .keyBy(_.id)
      .sum("temperature")

    srStream.print()

    env.execute("trans")
  }
}
