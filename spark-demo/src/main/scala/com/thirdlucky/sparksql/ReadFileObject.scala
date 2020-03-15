package com.thirdlucky.sparksql

import java.util.Properties

import org.apache.spark.sql.DataFrame

object ReadFileObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf}
    import org.apache.spark.sql.{SparkSession}
    val conf = new SparkConf().setAppName("spark-sql-udf").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val dfSql: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.227.135:3306/rdd?user")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()

    dfSql.show()

    val df: DataFrame = spark.read.format("json").load("input/json")
    df.show()

    df.write.format("parquet").mode("append").save("output/df")
    df.write.format("csv").mode("append").save("output/df.csv")
    df.write.format("json").mode("append").save("output/df.json")
    df.write.format("orc").mode("append").save("output/df.orc")
    df.write.format("rc").mode("append").save("output/df.rc")
    df.write.mode("append").jdbc("jdbc:mysql://192.168.227.135:3306/rdd?user=root&password=123456","user",new Properties())

    spark.stop
  }
}
