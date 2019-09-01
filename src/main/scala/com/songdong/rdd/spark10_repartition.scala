package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark10_repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，4个分区
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)


    listRDD.glom().collect().foreach(a=>println(a.mkString(",")))

    //重分区repartition  底层调用的就是coalesce。参数shuffle：Boolean设置为true，coalesce默认设置为false
    //2个分区
    val repartitionRDD: RDD[Int] = listRDD.repartition(2)

    repartitionRDD.glom().collect().foreach(a=>println(a.mkString(",")))

    sc.stop()


  }
}
