package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark09_coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16 ,4)


    println(listRDD.partitions.size)

    //缩减分区:可以简单的理解为合并分区

    val clalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println(clalesceRDD.partitions.size)


  }
}
