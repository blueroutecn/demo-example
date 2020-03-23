package com.thirdlucky.flinkdemo.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import com.thirdlucky.flinkdemo.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\sensor.txt")

    val srStream = stream.map(line => {
      val words: Array[String] = line.split(",")
      SensorReading(words(0), words(1).toLong, words(2).toDouble)
    })

    srStream.addSink(new MySqlSensorSink)
    env.execute("jdbc-sink")
  }
}

class MySqlSensorSink extends RichSinkFunction[SensorReading] {
  private var connection: Connection = _
  private var insert: PreparedStatement = _
  private var update: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = DriverManager.getConnection("jdbc:mysql://hadoop-node1:3306/test", "root", "123456")
    insert = connection.prepareStatement("insert into sensor_record (sensor,temp) values (?,?)")
    update = connection.prepareStatement("update sensor_record set temp = ? where sensor = ?")
  }


  override def close(): Unit = {
    insert.close()
    update.close()
    connection.close()
  }


  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    update.setDouble(1, value.temperature)
    update.setString(2, value.id)
    update.execute()

    if (update.getUpdateCount == 0) {
      insert.setString(1,value.id)
      insert.setDouble(2,value.temperature)
      insert.execute()
    }
  }
}
