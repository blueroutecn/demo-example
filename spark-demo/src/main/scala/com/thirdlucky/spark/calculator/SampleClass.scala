package com.thirdlucky.spark.calculator

object SampleClass {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-sample").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list = context.makeRDD(1 to 20)

    list.sample(false, 0.3).collect().foreach(println)

    list.takeSample(false, 6).foreach(println)
  }
}
