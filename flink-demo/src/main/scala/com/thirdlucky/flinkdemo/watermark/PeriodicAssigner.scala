package com.thirdlucky.flinkdemo.watermark

import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

object PeriodicAssignerTest {
  def main(args: Array[String]): Unit = {

  }
}

class MaxPeroidicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  var max: Long = Long.MinValue
  private val bouud: Long = 60 * 1000

  override def getCurrentWatermark: Watermark = {
    new Watermark(max-bouud)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    max = max.max(t.timestamp)
    max
  }
}

class SensorIDAssigner(prefix: String) extends AssignerWithPunctuatedWatermarks[SensorReading] {
  var bound: Long = 60 * 1000

  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if (t.id.startsWith(prefix)) {
      return new Watermark(l - bound)
    } else {
      return null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    return t.timestamp
  }
}


