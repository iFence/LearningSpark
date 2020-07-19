package it.aspirin.sparksql

import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @author yulei
 *         处理B站弹幕
 */
object DanmuHandle {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val spark = getSparkSession
    val df = addSource(spark)
    handle(df)
    val end = System.currentTimeMillis()
    println(s"花费时间：${end - start} ms")
  }

  def getSparkSession: SparkSession = SparkSession.builder().appName("danmu").master("local[*]").getOrCreate()

  def addSource(spark: SparkSession): DataFrame = {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    spark.read.jdbc("jdbc:mysql://localhost:3306/bilibili?characterEncoding=utf8&useSSL=true", "bf_all_dms", props)
  }

  def handle(df: DataFrame) = {
    val distinctDF = df.repartition(12).dropDuplicates(List("danmu_id"))
    val windowSpec = Window.partitionBy(col("send_id")).orderBy(col("send_time"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val countBySender = count(col("send_id")).over(windowSpec)
    distinctDF.select(col("send_id"), countBySender.alias("sum")).show(10000)
//    val l = distinctDF.count()
//    distinctDF
//      .groupBy("send_id")
//      .agg(count("send_id").as("sum"))
//      .select(col("send_id"), col("sum"))
//      .orderBy(col("sum").desc)
//      .filter(col("sum") >= 100)
//      .show(20)
    //    resDf.explain()
//    println(s"去重以后总共 $l 行")
  }
}
