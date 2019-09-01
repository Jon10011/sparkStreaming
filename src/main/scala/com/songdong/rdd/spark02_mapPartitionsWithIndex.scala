package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //mapPartitions算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)

    //mapPartitionsWithIndex
    val mapPartitionsWithIndixRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_,"分区号："+num))
      }
    }

    mapPartitionsWithIndixRDD.collect().foreach(println(_))


  }
}
