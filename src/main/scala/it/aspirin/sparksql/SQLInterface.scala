package it.aspirin.sparksql

import org.apache.spark.sql.SparkSession

object SQLInterface {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    doSQLTrans(spark)
  }

  def getSparkSession = SparkSession.builder().appName("SQL demo").master("local[*]").getOrCreate()

  def addCsvSource(spark: SparkSession) = {
    spark.read
      .option("inferSchema", true)
      .option("header", true)
      .option("mode", "dropMalformed")
      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/retail-data/all/online-retail-dataset.csv")
  }

  def doSQLTrans(spark: SparkSession) = {
    spark.sql("select 1 + 1").show()
  }

}
