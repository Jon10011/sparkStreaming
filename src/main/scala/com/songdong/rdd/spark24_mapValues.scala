package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark24_mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建k-vRDD，4个分区
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 4), ("b", 2), ("c", 3), ("d", 4), ("a", 3), ("b", 4), ("c", 8), ("c", 1), ("d", 1)), 2)

    val mapRDD: RDD[(Int, String)] = listRDD.map(a=>(a._2,a._1))

    val mapValuesRDD: RDD[(Int, String)] = mapRDD.mapValues(_+"***")
    //(1,a***),(4,a***),(2,b***),(3,c***),(4,d***),(3,a***),(4,b***),(8,c***),(1,c***),(1,d***)
    println(mapValuesRDD.collect().mkString(","))

    sc.stop()
  }
}