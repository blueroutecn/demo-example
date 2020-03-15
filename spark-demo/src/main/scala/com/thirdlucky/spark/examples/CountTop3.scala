package com.thirdlucky.spark.examples

object CountTop3 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}

    val conf = new SparkConf().setAppName("spark-top3").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list: RDD[String] = context.textFile("input/counttop3",5)

    // 1.筛选 删选符合条件的字符串，去掉 * 开头和 为空的字符串  1516609143867 6 7 64 16
    val filterRDD: RDD[String] = list.map(_.trim).filter(line => !line.startsWith("*") && line != "")

    // 2. 映射对象 ((pron,adv),1))
    val rdd = filterRDD.map(t => {
      val texts: Array[String] = t.split(" ")
      ((texts(1), texts(4)), 1)
    })

    // 3. 分区内外分组求和 ((pron, adv),count)
    val sumRDD: RDD[((String, String), Int)] = rdd.combineByKey(t => t, _ + _, _ + _)

    // 4. 分组 (proc,Iterable((adv,count)))
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = sumRDD.map(t => (t._1._1, (t._1._2, t._2))).groupByKey()

    // 5. 映射排序只留前三
    val top3RDD: RDD[(String, List[(String, Int)])] = groupRDD.map(t => {
      (t._1, t._2.toList.sortBy(-_._2).take(3))
    })

    top3RDD.collect.foreach(println)

    for (i <- 1 to 1000) {
      Thread.sleep(1000)
      println("wait" + i)
    }
  }
}


