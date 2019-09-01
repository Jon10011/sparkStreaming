package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark21_foladByKey{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建k-vRDD，4个分区
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",4),("b",2),("c",3),("d",4),("a",3),("b",4),("c",8),("c",1),("d",1)),2)

    listRDD.glom().collect().foreach(a=>println(a.mkString(",")))

    //floadByKey
    //求相同key的value相加
    /**
      * (d,5),(b,6)
      * (a,8),(c,12)
      */
    val foldRDD: RDD[(String, Int)] = listRDD.foldByKey(0)(_+_)
    foldRDD.glom().collect().foreach(a=>println(a.mkString(",")))


    sc.stop()
  }
}