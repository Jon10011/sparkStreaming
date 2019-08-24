package com.songdong.streaming.helloscalastreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  /**
    * 自定义接收器,无状态的转换
    */
  //接收器启动时调用
  override def onStart(): Unit = {
    new Thread("receiver") {
      override def run(): Unit = {
        //接收数据并提交给框架
        receive()
      }
    }.start()
  }

  def receive() = {
    var socket: Socket = null
    var input: String = null
    try {
      socket = new Socket(host, port)

      //生成输入流
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      //接收数据
      //      input = reader.readLine()
      //      store(input)

      //循环接收数据
      while (!isStopped() && (input = reader.readLine()) != null) {
        store(input)
      }
      //另一种写法
      //      while (!isStopped() && input != null){
      //        store(input)
      //        input = reader.readLine()
      //      }
      //重启
      restart("restart")
    } catch {
      case e: java.net.ConnectException => restart("restart")
      case t: Throwable => restart("restart")
    }
  }

  //接收器关闭时调用
  override def onStop(): Unit = {

  }
}
