package com.thirdlucky.flinkdemo.wordcount

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dsFile = env.readTextFile("D:\\scala\\spark\\demo-example\\flink-demo\\input\\hello.txt")
    val dsWordCount = dsFile.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    dsWordCount.print
  }
}
