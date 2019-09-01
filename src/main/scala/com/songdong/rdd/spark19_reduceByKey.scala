package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark19_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD: RDD[String] = sc.makeRDD(List("a", "b", "c", "d", "e", "f", "a", "c", "f"), 4)

    //转换成k-v类型
    val kvRDD: RDD[(String, Int)] = listRDD.map(a => (a, 1))

    //reduceByKey
    //相比较groupbyKey，reduceByKey多了一步预聚合操作，减少分区，性能提升
    val reduceRDD: RDD[(String, Int)] = kvRDD.reduceByKey(_+_)

    //(d,1),(e,1),(a,2),(b,1),(f,2),(c,2)
    println(reduceRDD.collect().mkString(","))

    sc.stop()
  }
}