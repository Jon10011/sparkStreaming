package com.songdong.test

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.streaming.{Seconds, StreamingContext}

object testStreaming {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("192.168.1.111",9999)

    val words = lines.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_ + _)


    //保存数据到外部数据库

    words.foreachRDD(rdd => rdd.foreachPartition(line =>{
      //加载driver 拓展可以自定义数据库连接池
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager
        .getConnection("jdbc:mysql://192.168.1.111:3306/test","root","songdong123")
      try{
        for(row <- line){
          val sql = "insert into webCount(titleName,count) values('"+row._1+"',"+row._2+")"
          conn.prepareStatement(sql).executeLargeUpdate()
        }
      }finally {
        conn.close()
      }
    }))

    words.print()
    ssc.start()

    ssc.awaitTermination()

  }
}
