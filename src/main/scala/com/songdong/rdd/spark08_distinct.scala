package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark08_distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 1, 2, 3, 4))

    //distinct 去重

    //数据被打乱重组，shuffle
    //val distinctRDD: RDD[Int] = listRDD.distinct()
    //去重后会导致数据减少，所以可以改变默认的分区数量，2表示去重后的数据存在两个分区中
    val distinctRDD: RDD[Int] = listRDD.distinct(2)


    distinctRDD.collect().foreach(println(_))


  }
}
