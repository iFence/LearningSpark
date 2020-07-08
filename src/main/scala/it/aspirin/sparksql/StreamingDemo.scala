package it.aspirin.sparksql

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 测试解析kafka中的数据写入MySQL
 */
object StreamingDemo {
  val brokerList = "localhost:9092"
  val topic = "test-old"
  case class ClickEvent(user:String, site:String, time:String)

  def main(args: Array[String]): Unit = {
    val spark = initSparkSession()
    val sourceDF = addkafkaSource(spark)
    doTransform(sourceDF, spark)
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder().master("local[*]").appName("kafka_to_mysql").getOrCreate()
  }

  def addkafkaSource(spark: SparkSession): DataFrame = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  def doTransform(sourceDf: DataFrame, spark:SparkSession) = {
    val schema = StructType(Array(
      StructField("user", StringType, false),
      StructField("site", StringType, false),
      StructField("time", StringType, false)
    ))
    val query = sourceDf
      .select(from_json(col("value").cast("string"), schema = schema).as("value"))
      .selectExpr("value.user","value.site","value.time")
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate",false)
      .format("console")
      .start()

    query.awaitTermination()
  }

  def startApplication(resDF: DataFrame): Unit = {

  }
}













