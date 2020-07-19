package it.aspirin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[6]").getOrCreate()
    spark.range(10).select(expr("*"), col("id")+lit("hel")).show(10)
  }

}
