package com.thirdlucky.spark.filepackage

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{JdbcRDD, RDD}

object mysqlObject {
  def connect(): Connection = {
    val driver: String = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.227.135:3306/rdd"
    val userName: String = "root"
    val password = "123456"
    //    Class.forName(driver)
    DriverManager.getConnection(url, userName, password)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-mysql").setMaster("local[*]")
    val context = new SparkContext(conf)

    //    select
    insert

    context.stop()

    def select(): Unit = {
      val sql = "select name,age from user where ? <= id and id <= ?"
      val jdbcRDD = new JdbcRDD(context, connect, sql, 1, 100, 2, resultOps)
      jdbcRDD.collect()
    }

    def insert(): Unit = {
      val rdd: RDD[(String, Int)] = context.makeRDD(List(("zhaoliu", 24), ("马7", 27)))
      rdd.foreachPartition(p => {
        val insertSql = "insert into user(name,age) values(?,?)"
        // 必须写在这里，否则无法序列化
        val connection: Connection = connect()
        p.foreach {
          case (name, age) => {
            val statement: PreparedStatement = connection.prepareStatement(insertSql)
            statement.setString(1, name)
            statement.setInt(2, age)
            statement.execute()
            statement.close()
          }
        }
        connection.close()
      })
    }

    def resultOps(rs: ResultSet) = {
      println(s"name=${rs.getString(1)},age=${rs.getInt(2)}")
    }
  }
}
