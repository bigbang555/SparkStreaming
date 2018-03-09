package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("hadoop00", 9999)

    val result = lines.flatMap(_.split(" ")).map((_, 1))

    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }


  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

}
