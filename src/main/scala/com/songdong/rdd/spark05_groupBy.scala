package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark05_groupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16)

    //groupBy,按照指定规则进行分组
    //分组后的数据形成了对偶数组（k-v），k表示分组的key，v表示分组的数据集合
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i%2)


    groupByRDD.collect().foreach(println(_))




  }
}
