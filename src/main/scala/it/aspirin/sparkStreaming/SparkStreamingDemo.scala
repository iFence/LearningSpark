package it.aspirin.sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object helloSparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val ssc = init()

    val lines = addSocketSource(ssc)
    val result = doTransform(lines)
    addConsoleSink(result)

    start(ssc)
  }

  def init(): StreamingContext = {
    val conf = new SparkConf().setAppName("spark streaming demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/Users/yulei/IdeaProjects/personal/LearningSpark/checkpoint/")
    new StreamingContext(sc, Seconds(5))
  }

  def addSocketSource(ssc: StreamingContext): InputDStream[String] ={
    ssc.socketTextStream("localhost", 9999)
  }

  /**
   * 对数据进行转换
   * @param inputStream
   * @return 结果Stream
   */
  def doTransform(inputStream:InputDStream[String]):DStream[(String,Int)] = {
    inputStream
      .flatMap(_.split(" "))
      .map((_, 1))
      //最重要的就是这个updateStateByKey函数，第一个参数表示某个key对应的value（也就是1）集合，option表示上次计算得到的值
      //这个函数的总体思路是将本次集合中的数据累加，并与上次的累计值累加
      .updateStateByKey((seq:Seq[Int], option:Option[Int]) =>{
        var value = 0
        value += option.getOrElse(0)
        for(e <- seq){
          value += e
        }
        Option(value)
      })
      .reduceByKey(_+_)
  }

  def addConsoleSink[String](transformedStream: DStream[String]): Unit ={
    transformedStream.print()
  }


  def start(ssc: StreamingContext): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }
}
