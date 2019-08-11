package com.songdong.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object testStreaming {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("192.168.1.111",9999)

    val words = lines.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

    words.print()
    ssc.start()

    ssc.awaitTermination()

  }
}
