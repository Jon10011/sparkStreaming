package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark12_union {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD01: RDD[Int] = sc.makeRDD(1 to 5, 4)
    val listRDD02: RDD[Int] = sc.makeRDD(5 to 10,3)

    //union 合并

    val unionRDD: RDD[Int] = listRDD01.union(listRDD02)


    unionRDD.collect().foreach(a=>println(a))


    sc.stop()


  }
}
