package it.aspirin.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BatchDemo1 {
  def main(args: Array[String]): Unit = {
    val sc = init()
    val initRDD = addTextSource(sc)
    val resRDD = doTransform(initRDD)
    addConsoleSink(resRDD)
  }

  def init(): SparkContext = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("BATCH DEMO")
    val sc = new SparkContext(conf)
    sc
  }

  def addTextSource(sc: SparkContext): RDD[String] = {
    sc.textFile("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data.txt")
  }

  def doTransform(initRDD: RDD[String]) = {
    initRDD.flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _).sortBy(_._2)
  }

  def addConsoleSink(resRDD: RDD[(String, Int)]): Unit = {
    resRDD.foreach(println)
  }
}
