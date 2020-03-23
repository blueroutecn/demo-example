package com.thirdlucky.flinkdemo.sink

import java.util
import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ESSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\sensor.txt")

    val srStream = stream.map(line => {
      val words: Array[String] = line.split(",")
      SensorReading(words(0), words(1).toLong, words(2).toDouble)
    })

    val hosts = new util.ArrayList[HttpHost]
    hosts.add(new HttpHost("hadoop-node1", 9200))

    val builder = new ElasticsearchSink.Builder[SensorReading](hosts, new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val json= new util.HashMap[String, String]
        json.put("id",t.id)
        json.put("tempeture",t.temperature.toString)
        json.put("timestamp",t.timestamp.toString)

        val request: IndexRequest = Requests.indexRequest()
          .index("sensor")
          .source(json)

        requestIndexer.add(request)
      }
    })

    srStream.addSink(builder.build())

    env.execute("es-sink")
  }
}
