package com.imooc.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]")
      .getOrCreate()

    val accessDF = spark.read.format("parquet").load("/users/cloudpc2c04XLw30/IdeaProjects/clean/")

    accessDF.printSchema()
    accessDF.show(false)

    videoAccessTopNStat(spark, accessDF)
    videoAccessTopNStatSQL(spark, accessDF)
    spark.stop()
  }

  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame) ={

    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    videoAccessTopNDF.show(false)
  }
  def videoAccessTopNStatSQL(spark: SparkSession, accessDF: DataFrame) ={

    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
    "where day='20170511' and cmsType='video' " +
    "group by day,cmsId order by times desc")

    videoAccessTopNDF.show(false)
  }
}
