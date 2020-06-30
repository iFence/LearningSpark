package it.aspirin.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * DataFrame聚合算子的使用
 */
object AggDemo {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    val spark = getSparkSession()
    val sourceDF = addCSVSource(spark)
//    doTransformation(sourceDF)
//    doVarAndStdTrans(sourceDF)
      doGroupingTrans(sourceDF)
    val endTime = System.currentTimeMillis()

    println(s"total time: ${endTime - startTime}")

  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder().appName("agg demo").master("local[3]").getOrCreate()
    spark
  }

  def addCSVSource(spark: SparkSession) = {
    spark.read
      .option("header", true)
      .option("inferSchema", true) //如果没有这句话，所有的数据都是string，有这句可以推断数据类型
      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/retail-data/all/online-retail-dataset.csv")
  }

  def doTransformation(sourceDF: DataFrame) = {
    sourceDF.persist()
    sourceDF.createTempView("dfTable")
    sourceDF
      .select(
        count(col("InvoiceNo")).as("count"),
        countDistinct(col("InvoiceNo")).as("distinct_count"),
        approx_count_distinct("StockCode", 0.1).as("appro_count"),
        max(col("UnitPrice")).as("max"),
        min(col("UnitPrice")).as("min"),
        sum(col("quantity")).as("sum"),
        sumDistinct(col("quantity")).as("sum_distinct"),
        avg("quantity").as("avg"),
        first("StockCode").as("first"),
        last("StockCode").as("last")
      )

      .show(5, false)
    println(s"总数据：${sourceDF.count()}")
  }

  def doVarAndStdTrans(sourceDF: DataFrame) = {
    sourceDF.select(var_pop("Quantity"), var_samp("Quantity"), stddev_pop("Quantity"), stddev_samp("Quantity"))
      .show(5, false)

    sourceDF.select(skewness("Quantity"), kurtosis("Quantity")).show(5)

    sourceDF.select(covar_pop("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"), corr("InvoiceNo", "Quantity")).show(false)
    sourceDF.agg(collect_set("Country"), collect_list("Country")).show()
  }

  def doGroupingTrans(sourceDF: DataFrame) = {
    sourceDF.groupBy("InvoiceNo", "CustomerId").count()/*.show(2)*/
    sourceDF.groupBy("InvoiceNo").agg(count("Quantity"), expr("count(Quantity)"), max("Quantity"))/*.show(2)*/
    sourceDF.groupBy("InvoiceNo").agg("Quantity"->"avg").show(5)
  }
}
