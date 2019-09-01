package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object spark17_partitionBy{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD01: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d")),4)
//    val listRDD02:RDD[(Int, String)] = sc.makeRDD(List((2,"a"),(3,"b"),(4,"c"),(5,"d")),4)

    listRDD01.glom().collect().foreach(a=>println(a.mkString(",")))

    //partitionBy 使用默认的分区期
    val partitionByRDD: RDD[(Int, String)] = listRDD01.partitionBy(new org.apache.spark.HashPartitioner(2))

    partitionByRDD.glom().collect().foreach(a=>println(a.mkString(",")))

    //使用自定义分区器
    val myPartitionerRDD: RDD[(Int, String)] = listRDD01.partitionBy(new MyPartitioner(3))
    myPartitionerRDD.glom().collect().foreach(a=>println(a.mkString(",")))


    sc.stop()
  }
}
//声明分区器
class MyPartitioner (partitions: Int) extends Partitioner{
  override def numPartitions: Int = {
    partitions
  }

  //定义分区规则
  override def getPartition(key: Any): Int = {
    1
  }
}
