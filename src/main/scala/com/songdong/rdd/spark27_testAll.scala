package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 数据结构 in/agent.log
  * 时间戳，省份，城市，用户，广告，中间字段使用空格分割
  *
  * 1518209043854804 8 75 66 10
  * 1521312435767777 5 66 60 18
  * 1511234354657676 2 14 18 7
  * 1518232878445665 1 57 19 16
  *
  */
object spark27_testAll {

  //需求：统计每一个省份广告被点击次数的TOP3
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val agentRDD: RDD[String] = sc.textFile("in/agent.log")


    val mapRDD: RDD[(String, Int)] = agentRDD
      .filter(!_.trim.equals(" "))
      .map(datas => (datas.split(" ")(1), datas.split(" ")(4).toInt))

    val reduceRDD: RDD[(Int, String)] = mapRDD.reduceByKey(_+_).map(a => (a._2,a._1))

    val sortRDD: RDD[(Int, String)] = reduceRDD.sortByKey(false)

    val result: Array[(String, Int)] = sortRDD.map(a=>(a._2,a._1)).take(3)

    println(result.mkString(","))


  }
}
