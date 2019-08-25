package com.songdong.streaming.Streaming_2

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming02_MyReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Streaming01_WordCount").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(5))


    //从自定义读取数据
    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("192.168.1.111",9999))


    //将采集的数据进行分解（扁平化）
    val wordDStrem: DStream[String] = receiverDStream.flatMap(line => line.split(" "))

    //将数据进行结构的转变方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStrem.map((_, 1))


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

// 声明自定义采集器

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var socket: Socket = null

  def receiver(): Unit = {
    socket = new Socket(host, port)

    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    var line: String = null

    while ((line = reader.readLine()) != null) {
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }

  }


  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receiver()
      }
    })

  }

  override def onStop(): Unit = {
    if (socket == null) {
      socket.close()
      socket = null
    }
  }
}















