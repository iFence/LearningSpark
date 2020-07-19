package it.aspirin.sparksql

import it.aspirin.sink.MysqlSink
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 测试解析kafka中的数据写入MySQL
 */
object StreamingDemo {
  val brokerList = "localhost:9092"
  val topic = "test-old"

  def main(args: Array[String]): Unit = {
    val spark = initSparkSession()
    val sourceDF = addKafkaSource(spark)
    val resDF = doTransform2(sourceDF)
        val query = sink2MySQL(resDF)
//    val query = sink2Console(resDF)
    startApplication(query)
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder().master("local[*]").appName("kafka_to_mysql").getOrCreate()
  }

  def addKafkaSource(spark: SparkSession): DataFrame = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  def doTransform(sourceDf: DataFrame) = {
    val schema = StructType(Array(
      StructField("user", StringType, false),
      StructField("site", StringType, false),
      StructField("time", StringType, false)
    ))
    //使用spark 函数 from_json解析kafka中的json数据
    val resDF = sourceDf
      .select(col("topic"), from_json(col("value").cast("string"), schema = schema).as("value"))
      .selectExpr("topic", "value.user", "value.site", "value.time")
    resDF
  }

  def doTransform2(sourceDF: DataFrame): DataFrame = {
    val schema = StructType(Array(
      StructField("user", StringType, false),
      StructField("site", StringType, false),
      StructField("time", StringType, false)
    ))
    sourceDF.select(col("topic"), from_json(col("value").cast("string"), schema = schema).as("value"))
      .selectExpr("topic", "value.user", "value.site", "value.time")
      .groupBy("user").agg(count("site").as("count"))
  }

  def sink2MySQL(resDF: DataFrame) = {
    val username = "root"
    val password = "root"
    val url = "jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf8&useSSL=true"
    val writer = new MysqlSink(url, username, password)
    val query = resDF.writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
    query
  }

  def sink2Console(resDF: DataFrame) = {
    resDF
      .writeStream
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
  }

  def startApplication(query: DataStreamWriter[Row]): Unit = {
    query.start().awaitTermination()
  }
}













