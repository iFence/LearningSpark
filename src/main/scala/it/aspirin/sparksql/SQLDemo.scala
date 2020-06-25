package it.aspirin.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLDemo {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: String)

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = getSparkSession()
    val sourceFrame = addCsvSource(spark)
    //    doTransform(spark, sourceFrame)
    doDataSetTransform(spark, sourceFrame)
    val endTime = System.currentTimeMillis()
    println(s"logging: start time: $startTime, end time: $endTime ,total time: ${endTime - startTime}")
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("spark session demo")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()
  }

  def addCollectionSource(spark: SparkSession): Dataset[Row] = {
    spark.range(1000).toDF("number")
  }

  def addCsvSource(spark: SparkSession) = {
    val summarys = spark
      .read
      //        .option("inferSchema", true)
      .option("header", true)
      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/csv/")

    summarys
  }

  def addParquetSource(spark: SparkSession) = {
    spark.read
      .parquet("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/parquet")
  }

  def doTransform(spark: SparkSession, sourceDataFrame: DataFrame) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //将数据注册成临时表
    sourceDataFrame.createTempView("summary")
    val res = spark.sql("select DEST_COUNTRY_NAME, count(1) as count from summary group by DEST_COUNTRY_NAME sort by count desc")
    sourceDataFrame
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .sort(desc("summary"))
      .show(5)
    res.explain()
  }

  def doDataSetTransform(spark: SparkSession, df: DataFrame) = {
    import spark.implicits._
    df.as[Flight]
      .explain()
//      .show(5)
  }

}




















