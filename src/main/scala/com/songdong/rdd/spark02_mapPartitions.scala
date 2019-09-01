package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //mapPartitions算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //mapPartition可以对一个RDD中所有的分区进行遍历
    //mapPartition效率优于map算子，减少了发送到执行器的交互次数
    //mapPartition可能会出现内存溢出（OOM）
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(x => {
      x.map(a => a * 2)
    })

    mapPartitionsRDD.collect().foreach(println(_))


  }
}
