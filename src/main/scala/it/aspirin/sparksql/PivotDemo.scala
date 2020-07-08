package it.aspirin.sparksql

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object PivotDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pivot").master("local[*]").getOrCreate()
    val df = addCsvSource(spark)
    //    doPivot(spark)
    doTransDoubleStream(df)
  }

  def addCsvSource(spark:SparkSession): DataFrame = {
    val schema = StructType(Array(
      StructField("InvoiceNo", LongType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", IntegerType, true),
      StructField("Quantity", DoubleType, true),
      StructField("InvoiceDate", StringType, true),
      StructField("UnitPrice", StringType),
      StructField("CustomerID", StringType, true),
      StructField("Country", StringType, true)
    ))
    spark.readStream.format("csv").schema(schema).csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/groupset.csv")
  }

  def doTransDoubleStream(df:DataFrame): Unit ={
    val df2 = df.select(
      col("InvoiceNo").as("InvoiceNo1"),
      col("StockCode").as("StockCode1"),
      col("Description").as("Description1"),
      col("Quantity").as("Quantity1"),
      col("InvoiceDate").as("InvoiceDate"),
      col("UnitPrice").as("UnitPrice1"),
      col("CustomerID").as("CustomerID"),
      col("Country").as("Country"))
    val stream = df.join(df2, df.col("InvoiceNo") === df2.col("InvoiceNo1"))
      .writeStream
      .format("console")
        .outputMode("append")

    stream.start()

  }

  def doPivot(spark: SparkSession) = {
    //构造一个DataFrame
    spark.createDataFrame(
      List(("2018-01", "项目1", 100),
        ("2018-01", "项目2", 200),
        ("2018-01", "项目3", 300),
        ("2018-02", "项目1", 1000),
        ("2018-02", "项目2", 2000),
        ("2018-03", "项目x", 999))
    )
      .groupBy(col("_1").as("时间"))
      .pivot("_2") //对项目列进行透视，被透视的列会变成新表的列名，当然如果下面要透视的是好几列，被透视的这一列会成为列的前缀
      .agg(sum("_3").as("s"), count("_3").as("c")) //对第三列进行聚合，聚合是按照第一列和第二列共同起作用的。即会分别计算1、2、3月项目1、2、3、x的总和
      .show()
  }
}
