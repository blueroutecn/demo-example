package com.thirdlucky.sparksql

import org.apache.spark.rdd.RDD

object JasonObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val conf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // from json
    val df = spark.read.json("input/json")

    // temp view from json
    df.createTempView("user")
    val frame: DataFrame = spark.sql("select * from user")
    frame.show()

    // global temp view
    df.createOrReplaceGlobalTempView("glob_user")
    val glob: DataFrame = spark.newSession().sql("select * from global_temp.glob_user")
    glob.show()

    // convert from rdd
    import spark.implicits._
    val list: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 12), (2, "lisi", 13), (3, "wangwu", 11)))
    val convert: DataFrame = list.toDF("id", "name", "age")
    convert.show

    spark.stop()
  }
}
