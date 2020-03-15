package com.thirdlucky.sparksql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction

object UdfSql {
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

    spark.udf.register("addname", (str: String) => "name:" + str)
    spark.sql("select addname(name),age from global_temp.user").show()

    spark.close()
  }
}
