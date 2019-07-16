package com.songdong.helloscalastreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCound {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("./check")

    //获取数据,监控92.168.1.111端口9999获取数据
    //    val lineDStream = ssc.socketTextStream("192.168.1.111",9999)

    //使用自定义接收器获取数据

    val lineDStream = ssc.receiverStream(new CustomerReceiver("192.168.1.111", 9999))
    //DStream[String]
    val wordsDStream = lineDStream.flatMap(_.split(" "))

    //DStream[(String,1)]
    val k2vDStream = wordsDStream.map((_, 1))

    //DStream[(String,sum)]
    //    val result = k2vDStream.reduceByKey(_+_)

    //保存上次的状态信息--》有状态的转换
    val updateFuc = (v: Seq[Int], state: Option[Int]) => {

      val preStatus = state.getOrElse(0)
      Some(preStatus + v.sum)
    }
    val result = k2vDStream.updateStateByKey(updateFuc)


    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
