package com.imooc.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Horizon
  * Time: 下午4:40 2018/2/21
  * Description:  SparkStreaming对接Kafka的方式二
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage : KakfaReceiverWordCount <brokers> <topics>")
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val sparkConf = new SparkConf()
      .setAppName("KakfaReceiverWordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))

    // TODO: SparkStreaming如何对接Kafka

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    val result = messages.map(_._2).map(_.split(" ")(1)).map(_.toInt/1000).map((_,1)).reduceByKey(_+_)

    result.print(20)

    ssc.start()
    ssc.awaitTermination()
  }

}
