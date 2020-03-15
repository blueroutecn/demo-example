package com.thirdlucky.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object CaseClassConvertObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val conf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    // from case class
    val list = spark.sparkContext.makeRDD(List(("zhangsan",1),("lisi",2),("wangwu",3)))
    val rdd: RDD[User] = list.map(t => User(t._1, t._2))
    val df: DataFrame = rdd.toDF
    df.show()

    // 转换 为 dataset
    val ds: Dataset[User] = df.as[User]

    ds.show()
    spark.stop()
  }
}

case class User(var name:String,var age:Int){

}
