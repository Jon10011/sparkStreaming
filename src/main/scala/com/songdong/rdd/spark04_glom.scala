package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark04_glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)


    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,3)

    //glom
    //将一个分区中的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()


    glomRDD.collect().foreach(array=>{
      println(array.mkString(","))
    })


  }
}
