package it.aspirin.sparksql

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object FirstDemo {

  case class Fight(dest_country_name: String, origin_country_name: String, count: Long)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("test json")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///")
      .getOrCreate()

    import session.implicits._
    val dataFrame = session.read.json("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/json/2010-summary.json")
    dataFrame
      .as[Fight]
      .filter(row => row.dest_country_name == "United States")
      .count()
  }
}
