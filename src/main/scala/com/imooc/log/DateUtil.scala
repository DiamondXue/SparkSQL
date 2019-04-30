package com.imooc.log

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date
import org.apache.commons.lang3.time.FastDateFormat

/**
  * date util
  */
object DateUtil {
  //10/Nov/2016:00:01:02 +0800
  //SimpleDateFormat is not thread safe, change to FastDateFormat

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //Target Format
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * get time
    */
  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * get input time: long
    * time: [10/Nov/2016:00:01:02 +0800]
    */
  def getTime(time: String) :Long = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }
}
