package com.imooc.spark

import java.sql.DriverManager

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("hadoop00", 9999)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //TODO: 将结果写到MySQL
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val connection = createConnection()
        partition.foreach(record => {
          val sql = "insert into wordcount values (?,?)"
          val ps = connection.prepareStatement(sql)
          ps.setString(1,record._1)
          ps.setInt(2,record._2)
          ps.executeUpdate()
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "idanlu")
  }

}
