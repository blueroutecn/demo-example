package com.thirdlucky.spark.keyValue


object joinObject {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list1 = context.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val list2 = context.makeRDD(List((1, 5), (2, 6), (3, 7),(5,8)))

    list1.join(list2).collect().foreach(println)

    list1.cogroup(list2).collect().foreach(println)

  }
}
