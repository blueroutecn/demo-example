package com.thirdlucky.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver

object UserDefineWordCountStreamingObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setAppName("spark-stream-test").setMaster("local[*]")
    val streaming = new StreamingContext(conf, Seconds(5))
    val fileStream: DStream[String] = streaming.receiverStream(new MyReceiver("192.168.227.135", 9999))
    val wordRDD: DStream[String] = fileStream.flatMap(_.split(" "))
    val tupleRDD: DStream[(String, Int)] = wordRDD.map((_, 1))
    val wordCount: DStream[(String, Int)] = tupleRDD.reduceByKey(_ + _)

    wordCount.print()
    // 启动采集器
    streaming.start()
    // Driver 等待采集器执行
    streaming.awaitTermination()
  }
}

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  var socket: Socket = null

  def startToReveive(): Unit = {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
    var line: String = null
    while ((line = reader.readLine()) != null) {
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        startToReveive
      }
    }).start()
  }

  override def onStop(): Unit = {
    socket.close()
  }
}
