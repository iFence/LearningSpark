package it.aspirin.sparksql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object WindowDemo {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val spark = getSparkSession
    val csvSource = addCsvSource(spark)
    //    doWindowTrans(csvSource)
    //    doGroupSetTrans(spark, csvSource)
    //    doPivot(spark, csvSource)
    //    val resDF = doReadMode(spark, csvSource)
    //    addCsvSink(resDF)
//    val jsonDF = addJSONSource(spark)
//    doJsonTrans(jsonDF)

    val end = System.currentTimeMillis()
    println(s"total time: ${end - start}")
  }

  def getSparkSession = SparkSession.builder().appName("window demo").master("local[*]").getOrCreate()

  def addCsvSource(spark: SparkSession) = {
    spark.read
      .option("inferSchema", true)
      .option("header", true)
      .option("mode", "dropMalformed")
      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/groupset.csv")

    //      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/retail-data/all/online-retail-dataset.csv")
  }

  def addJSONSource(spark: SparkSession) = {
    spark.read.format("json")
      .option("allowSingleQuotes", "false")
      .load("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/json.txt")
  }

  def addCsvSink(sinkDF: DataFrame): Unit = {
    sinkDF.write.format("csv")
      .mode("append")
      .save("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/output/out")
  }

  def doWindowTrans(csvSource: DataFrame) = {

    val windowSpec = Window
      .partitionBy("CustomerID")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    csvSource.where("CustomerID IS NOT NULL").orderBy("CustomerID")
      .select(col("CustomerID"), col("InvoiceDate"), col("Quantity"), purchaseDenseRank.as("quantityRank"), maxPurchaseQuantity.as("maxPurchaseQuantity"))
      .show()
  }

  def doGroupSetTrans(spark: SparkSession, source: DataFrame) = {
    source.drop().createTempView("dfTable")

    source.rollup("InvoiceNo", "StockCode", "Country").agg(sum("Quantity").as("total"))
      .selectExpr("InvoiceNo", "Country", "total")
      .orderBy("InvoiceNo").show()
    spark.sql(
      """
        |select CustomerId, stockCode, sum(Quantity), count(1) from dfTable
        |group by customerId, stockCode
        |order by customerId desc, stockCode desc
        |""".stripMargin).show(5)

    spark.sql(
      """
        |select CustomerID, stockCode, sum(Quantity), count(1) from dfTable
        |group by customerId, stockCode grouping sets((customerId, stockCode))
        |order by customerId desc, stockCode desc
        |""".stripMargin).show(5)

  }

  def doPivot(spark: SparkSession, source: DataFrame) = {
    //    source.groupBy("InvoiceNo").pivot("Country").sum().show()
    case class Pro(time: String, name: String, size: Int)
    spark.createDataFrame(
      List(("2018-01", "项目1", 100),
        ("2018-01", "项目2", 200),
        ("2018-01", "项目3", 300),
        ("2018-02", "项目1", 1000),
        ("2018-02", "项目2", 2000),
        ("2018-03", "项目x", 999))
    )
      .groupBy(col("_1").as("时间"))
      .pivot("_2")
      .agg(sum("_3"))
      .show()
  }

  def doReadMode(spark: SparkSession, source: DataFrame) = {
    source
  }

  def doJsonTrans(jsonDF: DataFrame) = {
    jsonDF.show(false)
  }
}
