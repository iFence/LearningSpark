package it.aspirin.sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingWithStateDemo {
  def main(args: Array[String]): Unit = {
    val ssc = init()

    val lines = addSocketSource(ssc)
    val result = doTransform2(lines)
    addConsoleSink(result)

    start(ssc)
  }

  def init(): StreamingContext = {
    val conf = new SparkConf().setAppName("spark streaming demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/Users/yulei/IdeaProjects/personal/LearningSpark/checkpoint/")
    new StreamingContext(sc, Seconds(5))
  }

  def addSocketSource(ssc: StreamingContext): InputDStream[String] = {
    ssc.socketTextStream("localhost", 9998)
  }

  /**
   * 测试 updateWithState 的使用
   * 对数据进行转换,其实是实现了数据的累加
   *
   * @param inputStream
   * @return 结果Stream
   */
  def doTransform(inputStream: InputDStream[String]): DStream[(String, Int)] = {
    inputStream
      .flatMap(_.split(" "))
      .map((_, 1))
      //最重要的就是这个updateStateByKey函数，第一个参数表示某个key对应的value（也就是1）集合，option表示上次计算得到的值
      //这个函数的总体思路是将本次集合中的数据累加，并与上次的累计值累加
      .updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
        //先计算本批次的总和 再累加之前批次的和
        val value = seq.sum + option.getOrElse(0)
        Option(value)
      })
      .reduceByKey(_ + _)
  }

  /**
   * 测试mapWithState函数的使用
   *
   * @param inputDStream
   * @return
   */
  def doTransform2(inputDStream: InputDStream[String]) = {
    //状态更新函数
    val mappingFunction = (currentKey: String, currentValue: Option[Int], previousState: State[Int]) => {
      //超时时处理，如果超时了就返回None
      if (previousState.isTimingOut()) {
        println("Key [" + currentKey + "] is timing out!")
        None
      } else {
        //没有超时时处理：没有超时就累加数据
        val currentCount = currentValue.getOrElse(0) + previousState.getOption().getOrElse(0)
        previousState.update(currentCount)
        (currentKey, currentCount)
      }
    }

    //mapWithState
    val result = inputDStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .mapWithState(
        StateSpec
          //设置更新函数
          .function(mappingFunction)
          //初始状态
          //          .initialState(initialStateRDD)
          //设置超时时间
          .timeout(Seconds(Integer.MAX_VALUE))
      )

    //输出全量状态
    result.stateSnapshots()
    //只输出有更新的状态
    //    result

  }


  def addConsoleSink[String](transformedStream: DStream[String]): Unit = {
    transformedStream.print()
  }


  def start(ssc: StreamingContext): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }
}
