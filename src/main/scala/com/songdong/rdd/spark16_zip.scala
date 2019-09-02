package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark16_zip {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD01: RDD[Int] = sc.makeRDD(1 to 5, 4)
    val listRDD02: RDD[Int] = sc.makeRDD(5 to 9, 4)

    //zip
    val zipRDD: RDD[(Int, Int)] = listRDD01.zip(listRDD02)

    zipRDD.collect().foreach(a => println(a))

    //    val a = Array(1,2,3)
    //    val b = Array("a","b","c","d")
    //    println(a.zip(b).mkString(","))


    sc.stop()


  }
}
