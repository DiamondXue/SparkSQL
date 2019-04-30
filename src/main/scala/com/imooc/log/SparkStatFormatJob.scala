package com.imooc.log

import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormat")
      .master("local[2]")
      .getOrCreate()

    //val access = spark.sparkContext.textFile("access.log")
    val access_20161111 = spark.sparkContext.textFile("access.20161111.log")
    //access.take(10).foreach(println)
    //access_20161111.take(10).foreach(println)
    access_20161111.map(line=> {
      val splits = line.split(" ")
      val ip=splits(0)

      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：[10/Nov/2016:00:01:02 +0800=> YYYY-MM-DD HH:MM:SS]
        */
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)

     // (ip, DateUtil.parse(time), url, traffic)

      DateUtil.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip

    })
      //.take(10).foreach(println)
        .saveAsTextFile("/output/")


    spark.stop()
  }
}
