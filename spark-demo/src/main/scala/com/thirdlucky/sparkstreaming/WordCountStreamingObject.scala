package com.thirdlucky.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCountStreamingObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.{SparkConf}
    val conf = new SparkConf().setAppName("spark-stream-test").setMaster("local[*]")
    val streaming = new StreamingContext(conf,Seconds(3))

    val receiver: ReceiverInputDStream[String] = streaming.socketTextStream("192.168.227.135", 9999)
    val wordStream: DStream[String] = receiver.flatMap(_.split(" "))
    val tupleStream: DStream[(String, Int)] = wordStream.map((_, 1))
    val wordCount: DStream[(String, Int)] = tupleStream.reduceByKey(_ + _)

    wordCount.print()

    // 启动采集器
    streaming.start()
    // Driver 等待采集器执行
    streaming.awaitTermination()
  }
}
