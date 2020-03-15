package com.thirdlucky.sparksql

import org.apache.spark.sql.{Dataset, Encoder, Encoders, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object AggregatorObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf}
    import org.apache.spark.sql.{SparkSession}
    val conf = new SparkConf().setAppName("spark-sql-udf").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val list = spark.sparkContext.makeRDD(List(("zhangsan", 1), ("lisi", 2), ("wangwu", 3)))
    val ds: Dataset[User] = list.map(t => User(t._1, t._2)).toDS
    ds.createOrReplaceGlobalTempView("user")

    val func = new AgeAveClassFunc
    val column: TypedColumn[User, Double] = func.toColumn.name("ageAVG")
    ds.select(column).show()

    spark.stop
  }
}

case class AgeAVE(var sum: Long, var count: Long)

class AgeAveClassFunc extends Aggregator[User, AgeAVE, Double] {
  override def zero: AgeAVE = {
    AgeAVE(0, 0)
  }

  override def reduce(b: AgeAVE, a: User): AgeAVE = {
    b.sum += a.age
    b.count += 1
    b
  }

  override def merge(b1: AgeAVE, b2: AgeAVE): AgeAVE = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: AgeAVE): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AgeAVE] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
