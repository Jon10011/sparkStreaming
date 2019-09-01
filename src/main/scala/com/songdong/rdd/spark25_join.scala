package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark25_join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建k-vRDD，4个分区
    val listRDD01: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d")),4)
    val listRDD02:RDD[(Int, String)] = sc.makeRDD(List((2,"a"),(3,"b"),(4,"c"),(5,"d")),2)


    val result: RDD[(Int, (String, String))] = listRDD01.join(listRDD02)

    //(4,(d,c)),(2,(b,a)),(3,(c,b))
    println(result.collect().mkString(","))

    sc.stop()
  }
}