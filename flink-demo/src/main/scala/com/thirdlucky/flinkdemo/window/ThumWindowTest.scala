package com.thirdlucky.flinkdemo.window

import com.thirdlucky.flinkdemo.sink.SensorMapper
import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{TimeCharacteristic, watermark}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object ThumWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 时间定期间隔 毫秒
    //    env.getConfig.setAutoWatermarkInterval(100L)

    val stream = env.socketTextStream("hadoop-node1", 7777)
    val srStream = stream.map(new SensorMapper)
      // 1秒一个窗口，提取的时间戳*1000（原来是毫秒，扩大为秒）
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    // 10秒一个窗口，取温度最小值
    srStream.map(r => (r.id, r.temperature)).keyBy(_._1)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .reduce((x, y) => (x._1, x._2.min(y._2)))
      .print("min")

    srStream.print("input")

    env.execute("thumling-window-test")
  }
}

class MyFixAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  var max: Long = Long.MinValue
  var bound: Int = 60 * 1000

  // 定期取得水位线的高度
  override def getCurrentWatermark: Watermark = {
    new watermark.Watermark(max - bound)
  }

  // 每次提取的时间戳
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    max = max.max(t.timestamp * 1000)
    t.timestamp * 1000
  }
}

// start开头的消息，以这个为水位线标准
class MyUnfixAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
  var bound: Long = 60 * 1000

  // 设置水位线
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if (t.id.startsWith("start")) {
      new watermark.Watermark(l - bound)
    } else {
      null
    }
  }

  // 每次提取的时间戳
  override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
}


