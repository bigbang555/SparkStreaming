package com.imooc.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLNetworkWordCount {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SQLNetworkWorldCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("hadoop00", 6789)
    val input = lines.flatMap(x => x.split(" "))
    input.foreachRDD((rdd, time) => {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val df = rdd.map(x => Record(x)).toDF()
      df.createOrReplaceTempView("words")
      val result = spark.sql("select word,count(1) as num from words group by word")
      println(s"========$time========")
      result.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

case class Record(word: String) {}

object SparkSessionSingleton {
  @transient
  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf) = {
    SparkSession.builder().config(sparkConf).getOrCreate()
  }
}
