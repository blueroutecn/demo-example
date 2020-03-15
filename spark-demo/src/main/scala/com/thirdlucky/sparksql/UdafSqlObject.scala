package com.thirdlucky.sparksql

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object UdafSqlObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf}
    import org.apache.spark.sql.{SparkSession}
    val conf = new SparkConf().setAppName("spark-sql-udf").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val list = spark.sparkContext.makeRDD(List(("zhangsan",1),("lisi",2),("wangwu",8)))
    val ds: Dataset[User] = list.map(t => User(t._1, t._2)).toDS()
    ds.createOrReplaceTempView("user")

    spark.udf.register("avgAge",new AvgAgeFunc)
    spark.sql("select avgAge(age) from user").show()

    spark.stop
  }
}

class AvgAgeFunc extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = new StructType().add("age",LongType)

  override def bufferSchema: StructType = new StructType().add("sum",LongType).add("count",LongType)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
