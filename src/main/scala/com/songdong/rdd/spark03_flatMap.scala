package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark03_flatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))

    //flatMap
    //1,2,3,4
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)


    flatMapRDD.collect().foreach(println(_))


  }
}
