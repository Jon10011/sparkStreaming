package com.songdong.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark07_sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16)

    //sample 抽样
    //从指定数据集合中进行抽样处理，根据不同的算法抽样
    //true/false表示采样的数据是否放回再次抽样
    val sampleRDD: RDD[Int] = listRDD.sample(false,0.4,2)

    sampleRDD.collect().foreach(println(_))




  }
}
