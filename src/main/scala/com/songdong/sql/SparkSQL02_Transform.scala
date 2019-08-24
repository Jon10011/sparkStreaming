package com.songdong.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL02_Transform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //    val sc = new SparkContext(conf)
    //    val spark1: SparkSession = new SparkSession(sc,)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //引入隐式转换，增加函数
    //这里的spark是指sparksession对象的名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40), (3, "wangwu", 50)))

    //转换DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    //转换DS
    val ds: Dataset[User] = df.as[User]

    //转换为DF
    val df2: DataFrame = ds.toDF()

    //转换为RDD
    val rdd2: RDD[Row] = df2.rdd

    rdd2.foreach(row =>{
      //获取数据时，可以通过索引获取
      println(row.get(1))
    })

    spark.stop()


  }
}

//样例类

case class User(id: Int, name: String, age: Int)