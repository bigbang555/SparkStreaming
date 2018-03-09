package com.imooc.spark.project.dao

import com.imooc.spark.project.HBaseUtils
import com.imooc.spark.project.domain.CourseSearchClickCount
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Horizon 
  * Time: 下午1:29 2018/2/25
  * Description: 
  */
object CourseSearchClickCountDAO {
  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val column = "click_count"

  def save(list: ListBuffer[CourseSearchClickCount]) = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course), Bytes.toBytes(cf), Bytes.toBytes(column), ele.clickc_count)
    }

  }

  def count(day_search_course: String) = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_search_course))
    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(column))
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    println(count("20180228_yahoo.com_112"))
    println(count("20180228_yahoo.com_128"))
    println(count("20180228_yahoo.com_130"))
    println(count("20180228_yahoo.com_131"))
    println(count("20180228_yahoo.com_145"))
    println(count("20180228_yahoo.com_146"))
  }

}
