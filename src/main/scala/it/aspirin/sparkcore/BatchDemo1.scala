package it.aspirin.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BatchDemo1 {
  def main(args: Array[String]): Unit = {
    val sc = init()
    val initRDD = addTextSource(sc)
//    val resRDD = doTransform(initRDD)
//    addConsoleSink(resRDD)
    doTransform2(sc,initRDD)
  }

  def init(): SparkContext = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("BATCH DEMO")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/Users/yulei/IdeaProjects/personal/LearningSpark/checkpoint")
    sc
  }

  def addTextSource(sc: SparkContext): RDD[String] = {
    sc.textFile("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/data.txt")
  }

  def doTransform(initRDD: RDD[String]) = {
    initRDD.flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _).sortBy(_._2)
  }

  def doTransform2(sc: SparkContext, sourceRDD: RDD[String]) = {
//    sourceRDD.saveAsTextFile("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/output/aa")
//    sourceRDD.saveAsObjectFile("/Users/yulei/IdeaProjects/personal/LearningSpark/src/main/resources/output/aa-par")
    println(sourceRDD.pipe("python /Users/yulei/PycharmProjects/MyScripts/learning_arrow/main.py").collect().toList)
    println(sourceRDD.pipe("wc -l").collect().toList(0))
    println(sourceRDD.mapPartitions(par => Iterator[Int](1)).sum())
  }

  def addConsoleSink(resRDD: RDD[(String, Int)]): Unit = {
    resRDD.foreach(println)
  }
}
