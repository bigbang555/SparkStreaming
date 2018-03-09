package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val ssc = new StreamingContext(sc, Seconds(5))

    val blacks = sc.parallelize(List("zhangsan", "lisi"))
    val blacksRDD = blacks.map(x => (x, true))

    val lines = ssc.socketTextStream("hadoop00", 6789)
    lines.foreachRDD(rdd=>{

    })
    val result = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD).filter(x => x._2._2.getOrElse(false) == false).map(x => x._2._1)
    })
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
