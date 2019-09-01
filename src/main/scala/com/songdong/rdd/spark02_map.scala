package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //所有RDD算子的计算功能全部由Executor执行spark02_map$
    val mapRDD: RDD[Int] = listRDD.map(x => x * 2)

    mapRDD.collect().foreach(println(_))


  }
}
