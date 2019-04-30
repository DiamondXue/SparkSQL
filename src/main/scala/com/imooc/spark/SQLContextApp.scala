package com.imooc.spark

import org.apache.spark.sql.SparkSession

object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val path="scores.csv"
    //1) create Context
    val spark = SparkSession.builder()
      .appName("App")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
        .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load(path).toDF()

    df.show

    spark.stop()

  }
}
