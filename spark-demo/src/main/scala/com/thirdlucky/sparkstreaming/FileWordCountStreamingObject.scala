package com.thirdlucky.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object FileWordCountStreamingObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setAppName("spark-stream-test").setMaster("local[*]")
    val streaming = new StreamingContext(conf,Seconds(5))
    val fileStream: DStream[String] = streaming.textFileStream("streamsource")
    val wordStream: DStream[String] = fileStream.flatMap(_.split(" "))
    val tupleStream: DStream[(String, Int)] = wordStream.map((_, 1))
    val wordCount: DStream[(String, Int)] = tupleStream.reduceByKey(_ + _)

    wordCount.print()

    // 启动采集器
    streaming.start()
    // Driver 等待采集器执行
    streaming.awaitTermination()
  }
}
