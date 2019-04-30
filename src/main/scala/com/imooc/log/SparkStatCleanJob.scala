package com.imooc.log

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
/**
  * use Spark to clean data
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatClean")
      .master("local")
      .getOrCreate()

    val accessRDD = spark.sparkContext.textFile("/users/cloudpc2c04XLw30/IdeaProjects/data/access.log")
    accessRDD.take(10).foreach(println)

    //RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AcccessConverUtil.parseLog(x)).filter(x=> x.equals(Row(0)).unary_!),
      AcccessConverUtil.struct)

    //accessDF.printSchema()
    //accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
        .save("/users/cloudpc2c04XLw30/IdeaProjects/clean/")
    spark.stop()
  }

}
