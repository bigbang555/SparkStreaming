package com.imooc.spark.project.dao

import com.imooc.spark.project.HBaseUtils
import com.imooc.spark.project.domain.CourseClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Horizon 
  * Time: 下午9:55 2018/2/24
  * Description: 
  */
object CourseClickCountDAO {
  val tableName = "imooc_cource_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  def save(list: ListBuffer[CourseClickCount]) = {
    val table = HBaseUtils.getInstance().getTable("imooc_cource_clickcount")
    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), ele.click_count)
    }
  }

  def count(day_course: String) = {
    val table = HBaseUtils.getInstance().getTable("imooc_cource_clickcount")
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {

    println(count("20180226_112")+" : "+count("20180226_128")+" : "+count("20180226_130")+" : "+count("20180226_131")+" : "+count("20180226_145")+" : "+count("20180226_146")+" : ")
  }

}
