package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark06_filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16)

    //filter，按照指定规则过滤

    val filterRDD: RDD[Int] = listRDD.filter(x => x%2==0)

    filterRDD.collect().foreach(println(_))




  }
}
