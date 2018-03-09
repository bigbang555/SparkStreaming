package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming 处理文件系统（local/hdfs）的数据
  **/
object FileWordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    val lines = ssc.textFileStream("file:///Users/zhaoyan/Documents/Atom/ss")
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()


    ssc.start()
    ssc.awaitTermination()

  }
}
