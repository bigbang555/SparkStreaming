package com.imooc.spark.project.spark

import com.imooc.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.imooc.spark.project.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Horizon 
  * Time: 下午8:09 2018/2/24
  * Description: 
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage : ImoocStatStreamingApp <brokers> <topics>")
      System.exit(1)
    }
    val Array(brokers,topics) = args

    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("ImoocStatStreamingApp")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))
    val kafkaParams = Map("metadata.broker.list"->brokers)
    val topicSet = topics.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)
    val log = messages.map(_._2)
    //   63.156.187.55	2018-02-24 21:06:01	"GET /class/112.html HTTP/1.1"	-	200

    val cleanData = log.map(line => {
      val infos = line.split("\t")
      var courceId = 0
      val ip = infos(0)
      val date = DateUtils.parse(infos(1))
      val url = infos(2)
      val refer = infos(3)
      val status = infos(4).toInt
      val url_block_info = url.split(" ")(1).split("/")
      if (url_block_info(1).equals("class")) {
        courceId = url_block_info(2).substring(0, url_block_info(2).lastIndexOf(".")).toInt
      }
      ClickLog(ip, date, courceId, status, refer)
    }).filter(click => click.courceId != 0)


    //统计到今天为止的课程访问量
    val day_course = cleanData.map(x => {
      val day_course = x.date.substring(0, 8) + "_" + x.courceId
      (day_course, 1)
    }).reduceByKey(_ + _)

    day_course.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list = new ListBuffer[CourseClickCount]
        partition.foreach(rdd => {
          list.append(CourseClickCount(rdd._1, rdd._2))
        })
        CourseClickCountDAO.save(list)
      })
    })
    // 统计从搜索引擎过来的访问量 http://cn.bing.com/search?q=Storm实战
    val day_search_course = cleanData.filter(x => x.refer.equals("-") != true).map(x => {
      val date = x.date.substring(0, 8)
      var refer = ""
      if (x.refer.equals("-")) {
        refer = "-"
      } else {
        refer = x.refer.replaceAll("//","/").split("/")(1)
      }
      val course = x.courceId
      (date + "_" + refer + "_" + course, 1)
    }).reduceByKey(_ + _)

    day_search_course.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list = new ListBuffer[CourseSearchClickCount]
        partition.foreach(rdd => {
          list.append(CourseSearchClickCount(rdd._1, rdd._2))
        })
        CourseSearchClickCountDAO.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
