package com.thirdlucky.flinkdemo.transfer

import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.streaming.api.scala._

object SplitTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\sensor.txt")

    val srStream = stream.map(line => {
      val words: Array[String] = line.split(",")
      SensorReading(words(0), words(1).toLong, words(2).toDouble)
    })

    val splitStream = srStream.split(r => {
      if (r.temperature > 30) Seq("hign")
      else Seq("low")
    })
    val hign = splitStream.select("hign")
    val low = splitStream.select("low")
    val all = splitStream.select("hign", "low")

    hign.union(low,all).print()

//    val lessHign: DataStream[(String, Double)] = hign.map(r => (r.id, r.temperature))
//    val conStream: ConnectedStreams[(String, Double), SensorReading] = lessHign.connect(low)
//    val coMapStream = conStream.map(lh => lh, low => low)
//
//    coMapStream.print()
    //    hign.print("hign")
    //    low.print("low")
    //    all.print("all")

    env.execute("trans")
  }
}
