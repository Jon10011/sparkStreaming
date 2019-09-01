package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object spark18_groupByKey{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD: RDD[String] = sc.makeRDD(List("a", "b", "c", "d", "e", "f","a", "b", "c", "d", "e", "f", "a", "c", "f"),4)

    //转换成k-v类型
    val kvRDD: RDD[(String, Int)] = listRDD.map(a=>(a,1))

    //groupByKey
    val groupRDD: RDD[(String, Iterable[Int])] = kvRDD.groupByKey()

    //(d,CompactBuffer(1, 1)),(e,CompactBuffer(1, 1)),(a,CompactBuffer(1, 1, 1)),(b,CompactBuffer(1, 1)),(f,CompactBuffer(1, 1, 1)),(c,CompactBuffer(1, 1, 1))
    println(groupRDD.collect().mkString(","))

    sc.stop()
  }
}