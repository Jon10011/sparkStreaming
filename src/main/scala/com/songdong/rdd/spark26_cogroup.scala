package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark26_cogroup{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建k-vRDD，4个分区
    val listRDD01: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d")),4)
    val listRDD02:RDD[(Int, String)] = sc.makeRDD(List((2,"a"),(3,"b"),(4,"c"),(5,"d")),2)

    //join
    //(4,(d,c)),(2,(b,a)),(3,(c,b))
    val result: RDD[(Int, (String, String))] = listRDD01.join(listRDD02)
    println(result.collect().mkString(","))

    //cogroup
    //(4,(CompactBuffer(d),CompactBuffer(c))),(1,(CompactBuffer(a),CompactBuffer())),(5,(CompactBuffer(),CompactBuffer(d))),(2,(CompactBuffer(b),CompactBuffer(a))),(3,(CompactBuffer(c),CompactBuffer(b)))
    val result1: RDD[(Int, (Iterable[String], Iterable[String]))] = listRDD01.cogroup(listRDD02)
    println(result1.collect().mkString(","))



    sc.stop()
  }
}