package com.imooc.spark

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Horizon 
  * Time: 下午4:40 2018/2/21
  * Description:  SparkStreaming对接Kafka的方式一
  */
object KakfaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage : KakfaReceiverWordCount <zkQuorum> <group> <topics> <numThread>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThread) = args
    val sparkConf = new SparkConf()
      .setAppName("KakfaReceiverWordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))

    // TODO: SparkStreaming如何对接Kafka

    val topicMap = topics.split(",").map((_,numThread.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

    val result = messages.map(_._2).map(_.trim).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
