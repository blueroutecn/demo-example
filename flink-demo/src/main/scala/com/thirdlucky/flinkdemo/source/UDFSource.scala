package com.thirdlucky.flinkdemo.source

import java.util.Properties
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import scala.util.Random

object UDFSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream.print()

    env.execute("kafka-source")
  }

  class SensorSource extends SourceFunction[SensorReading] {
    var running: Boolean = true

    override def run(context: SourceFunction.SourceContext[SensorReading]): Unit = {
      val random = new Random

      while (running) {
        (1 to 10)
          .map(i => SensorReading(s"sensor_${i}", System.currentTimeMillis(), random.nextGaussian() * 30 + 20))
          .foreach(context.collect)

        Thread.sleep(500)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

}
