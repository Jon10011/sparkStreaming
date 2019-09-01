package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark22_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建k-vRDD，4个分区
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 4), ("b", 2), ("c", 3), ("d", 4), ("a", 3), ("b", 4), ("c", 8), ("c", 1), ("d", 1)), 2)

    listRDD.glom().collect().foreach(a => println(a.mkString(",")))

    //combineByKey
    //需求：根据key计算每种key的均值。（计算每个key出现的次数以及对应值的总和，再相除得到结果）
    /**
      * (d,5),(b,6)
      * (a,8),(c,12)
      */
    val combineRDD: RDD[(String, (Int, Int))] = listRDD.combineByKey(
      (_, 1),
      (datas: (Int, Int), v) => (datas._1 + v, datas._2 + 1),
      (datas: (Int, Int), datas2: (Int, Int)) => (datas2._1 + datas._1, datas2._2 + datas2._2)
    )
    combineRDD.collect().foreach(println(_))


    val result: RDD[(String, Double)] = combineRDD.map {
      case (k, v) => (k, v._1 / v._2.toDouble)
    }
    println(result.collect().mkString(","))//.foreach(println(_))

    sc.stop()
  }
}