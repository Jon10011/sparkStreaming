package com.songdong.streaming.Streaming_2

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Streaming01_WordCount").setMaster("local[*]")
    val StreamingContext: StreamingContext = new StreamingContext(conf, Seconds(5))


    //从指定端口读取数据
    val socketLineDStream: ReceiverInputDStream[String] = StreamingContext.socketTextStream("192.168.1.111", 9999)


    //将采集的数据进行分解（扁平化）
    val wordDStrem: DStream[String] = socketLineDStream.flatMap(line => line.split(" "))

    //将数据进行结构的转变方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStrem.map((_, 1))


    //将转换后的数据进行聚合处理
    val wordSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    //打印
    wordSumDStream.print()


    //不能停止采集程序
    //StreamingContext.stop()

    //启动采集器
    StreamingContext.start()

    //Driver等待采集器执行
    StreamingContext.awaitTermination()

  }
}
