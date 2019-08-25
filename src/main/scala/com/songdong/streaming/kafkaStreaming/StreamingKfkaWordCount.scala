package com.songdong.streaming.kafkaStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKfkaWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StreamingKafkaWordCount").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val zkQuorum = "192.168.1.111:2181,192.168.1.112:2181"
    val groupID = "g1"
    val topic = Map[String ,Int]("xiaoniu" -> 1)

    //创建DStream 需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupID,topic)

    //对数据进行处理
    //kafka 的 ReceiverInputDStream[(String, String)]里面装的是一个元祖（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)
    //对DStream进行操作，操作一个代理（描述）就像操作一个本地集合一样
    //切分、压平
    val words: DStream[String]= lines.flatMap(_.split(" "))

    //单词组合在一起
    val wordAndOne:DStream[(String,Int)] = words.map((_,1))

    //聚合
    val reduced: DStream[(String, Int)]= wordAndOne.reduceByKey(_+_)

    //打印结果
    reduced.print()

    //启动SparkStreaming程序
    ssc.start()

    //等待优雅的退出
    ssc.awaitTermination()


  }
}
