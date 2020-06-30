package it.aspirin.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 一些基本的DataFrame函数的使用
 */
object BasicDemo {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: String)

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = getSparkSession()
    val sourceFrame = addCsvSource(spark)
    //    doTransform(spark, sourceFrame)
    //    doDataSetTransform(spark, sourceFrame)
    //    doJsonTrans(spark, sourceFrame)
    //    doBasicTrans(spark, sourceFrame)
    //    doJsonTransform(spark, sourceFrame)
    //    doDateTrans(spark, sourceFrame)
    //    doNullValTrans(spark, sourceFrame)
    doNullValTrans2(spark)
    //    doMapTrans(spark, sourceFrame)
    //    doJsonTrans(spark)
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
    val schema = StructType(Array(
      StructField("d_name", StringType, true),
      StructField("o_name", StringType, true),
      StructField("count", IntegerType, true)
    ))
    val summarys = spark
      .read
      //        .option("inferSchema", true)
      .option("header", true)
      .schema(schema)
      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/csv/2011-summary.csv")

    summarys
  }

  def addParquetSource(spark: SparkSession) = {
    spark.read
      .parquet("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/parquet")
  }

  def addJsonSource(spark: SparkSession) = {
    spark.read.json("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/json")
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

  /**
   * 对读取的json格式的数据进行转换
   *
   * @param sourceDF
   */
  def doJsonTransform(spark: SparkSession, sourceDF: DataFrame) = {
    import spark.implicits._
    sourceDF
      //selectExpr里面可以写函数，比如下面的cast，但是select只能写列名
      .selectExpr("cast(count as int) count", "d_name", "O_NAME")
      .groupBy("d_name")
      .agg(sum("count").as("total"), max("count").as("max"))
      //表达列的四种方式，前两种在[org.apache.spark.sql.functions._]中，Python，Java也这么用，后两种属于隐式转换，是Scala独有的
      .select(col("d_name"), column("total"), $"max", 'max as "max2")
      .select(expr("d_name"), expr("max - 100"), expr("max") - 100, col("max") - 100)
      .columns.foreach(println)

    //      .show()
  }

  def doJsonTrans(spark: SparkSession, sourceDF: DataFrame) = {
    import spark.implicits._
    sourceDF
      .select(col("d_name").cast("string"), col("count").cast("long"))
      .orderBy(expr("d_name").desc)
      .withColumnRenamed("d_name", "dest_country_name")
      .withColumn("this long column name", expr("dest_country_name"))
      .selectExpr("`this long column name`", "`this long column name`  as `new col`")
      .show()
  }

  def doBasicTrans(spark: SparkSession, sourceDF: DataFrame) = {

    import spark.implicits._
    sourceDF
      .select(expr("*"), lit(1).alias("one"))
      .withColumn("colName", lit("hello"))
      .withColumn("this long column name", lit("long long"))
      .where(col("o_name").isin("Ireland", "Singapore") and col("d_name").like("Uni%"))
      .sample(false, 0.6, 5).repartition(col("o_name"))
      .show(false)

    sourceDF.createTempView("dfTable")
    spark.sql("select d_name, o_name, count from dfTable")
      .select(col("count") + 100, expr("count - 100"), expr("count") * 20, column("d_name"), $"o_name", 'count)
      .where(expr("d_name = 'shanghai' or o_name = 'beijing'"))
      .where(col("d_name").equalTo("shanghai"))
      .select(round(lit(3.2)), bround(lit(3.8)), bround(lit(3.2)), round(lit(3.8)))
      .show(5)

    val dataframes: Array[DataFrame] = sourceDF.randomSplit(Array(0.8, 0.2), 10)
  }

  def doDateTrans(spark: SparkSession, sourceDF: DataFrame) = {
    spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
      .select(date_sub(col("today"), 5).as("sub"), col("today"),
        col("now"), date_add(col("now"), 5).as("add"))
      .withColumn("date", lit("2020-06-27"))
      .select(to_date(col("date")).as("da"), to_timestamp(col("date"), "yyyy-MM-dd").as("ti"))
      .select(col("da") < col("ti"))
      //      .select(datediff(col("add"), col("sub")))
      //      .explain()
      .show(1, false)
  }

  def doNullValTrans(spark: SparkSession, sourceDF: DataFrame) = {
    //    sourceDF.select(coalesce(col("d_name"),col("O_NAME"), col("count"))).show(1,false)
    sourceDF.na
      .replace("o_name", Map("null" -> "hahah"))
      .orderBy(col("count").desc_nulls_last)
      //      .select(struct("d_name", "o_name").as("dest_origin"), col("count"))
      .selectExpr("(d_name, o_name) as complex", "count")
      //      .select("complex.d_name", "count")
      //      .select(col("complex").getField("o_name"))
      .select("complex.*")
      .withColumn("longCol", lit("this is a long column"))
      .select(split(col("longCol"), " ").as("list"))
      .select(expr("list[0]"), expr("list[1]"),
        size(col("list")).as("len"),
        array_contains(col("list"), "is"))
      .show(5, false)
    //spark中注册的表是不区分大小写的
    sourceDF.createTempView("handleNull")
    spark.sql(
      """
        |select ifnull('null', 'return_value'),
        |nullif('hello', null),
        |nvl('null1', 'return_value'),
        |nvl2('', 'return_value',"else_value")
        |from handlenull limit 1
        |""".stripMargin).show(false)
  }

  def doNullValTrans2(spark: SparkSession) = {
    spark.read
      .option("header", true)
      .csv("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data/flight-data/csv/2010-summary.csv")
      .select(col("DEST_COUNTRY_NAME"), col("ORIGIN_COUNTRY_NAME"), col("count").cast("long").isNaN)
      .show(5, false)
//      .printSchema()
  }

  def doJsonTrans(spark: SparkSession) = {
    spark.range(1).selectExpr(
      """
        |'{"Jkey":{"Jvalue":"[1,2,3]"}}' as col1
        |""".stripMargin, """'{"key":"value2"}' as col2""")
      .select(get_json_object(col("col1"), "$.Jkey.Jvalue") as "col1",
        get_json_object(col("col2"), "$.key") as "col2",
        json_tuple(col("col1"), "Jkey") as "tuple")
      .show(false)
  }

  def doMapTrans(spark: SparkSession, sourceDF: DataFrame) = {
    sourceDF.select(map(col("o_name"), col("d_name")).as("complex_map"))
      .select(explode(col("complex_map")))
      //      .select(to_json(struct(col("key"), col("value"))) as "struct_json")
      //      .withColumn("arr", lit("this is a array"))
      //      .select(col("key"), col("value"), split(col("arr"), " ") as ("list"))
      //      .select(explode(col("list")))
      //      .select(explode(col("exp")))
      .show(5, false)
  }

}




















