package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark20_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建k-vRDD，4个分区
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",4),("b",2),("c",3),("d",4),("a",3),("b",4),("c",8),("d",1)),2)

    listRDD.glom().collect().foreach(a=>println(a.mkString(",")))

    //aggregateByKey
    //求每个分区相同key对应值的最大值，然后相加
    val res: RDD[(String, Int)] = listRDD.aggregateByKey(0)(math.max(_,_),_+_)
    res.glom().collect().foreach(a=>println(a.mkString(",")))


    sc.stop()
  }
}