package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage:  FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname, port) = args
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))

    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)
    val result = flumeStream.map(x => new String(x.event.getBody.array()).trim).flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
