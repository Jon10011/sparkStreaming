package com.songdong.streaming.Streaming_2

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming04_Window {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Streaming01_WordCount").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从kafka数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "192.168.1.111:2181",
      "test0825",
      Map("test0825" -> 3)
    )


    //窗口大小应该是采集周期的整数倍,窗口滑动的步长也应该为采集周期的整数倍
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9),Seconds(3))

    //将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = windowDStream.flatMap(t=>t._2.split(" "))


    //将数据进行结构的转变方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))


    //将转换后的数据进行聚合处理
    val wordSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    //打印
    wordSumDStream.print()


    //不能停止采集程序
    //StreamingContext.stop()

    //启动采集器
    streamingContext.start()

    //Driver等待采集器执行
    streamingContext.awaitTermination()

  }
}













