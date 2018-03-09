package com.imooc.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

import scala.sys.process.ProcessBuilder.Source

/**
  * Created by Horizon 
  * Time: 下午8:41 2018/2/24
  * Description: 
  */
object DateUtils {
  val SOURCE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def parse(source:String) = {
    TARGET_FORMAT.format(new Date(getSource(source)))
  }

  def getSource(source: String)={
    SOURCE_FORMAT.parse(source).getTime
  }

}
