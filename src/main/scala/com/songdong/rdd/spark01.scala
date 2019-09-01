package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark01").setMaster("local[*]")
    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //从内存创建makeRDD,底层实现就是parallelize,默认分区取核数与2的最大值
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//    val listRDD1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

//    //从内存中创建parallelize,并行
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))
//
//    //从外部存储创建，默认分区取核数与2的最小值
//    //默认情况下可以读取项目路径。也可以读取其他路径：：hdfs
//    //默认文件中读取的数据都是字符串类型
    //读取文件时传递的分区参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件时的分片规则
    val fileRDD: RDD[String] = sc.textFile("in")
//    val fileRDD1: RDD[String] = sc.textFile("in",3)

//    listRDD.collect().foreach(println(_))

    listRDD.saveAsTextFile("output")
  }

}
