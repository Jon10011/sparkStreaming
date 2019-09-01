package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark11_sortBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    //排序

    //默认升序
    val sortByRDD01: RDD[Int] = listRDD.sortBy(x=>x)
    //降序
    val sortByRDD02: RDD[Int] = listRDD.sortBy(x=>x,false)


    sortByRDD01.collect().foreach(println(_))
    sortByRDD02.collect().foreach(println(_))

    sc.stop()


  }
}
