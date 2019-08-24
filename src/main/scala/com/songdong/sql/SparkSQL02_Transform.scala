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

    //RDD转换DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    //DF转换DS
    val ds: Dataset[User] = df.as[User]

    //DS转换为DF
    val df2: DataFrame = ds.toDF()

    //DF转换为RDD
    val rdd2: RDD[Row] = df2.rdd

    rdd2.foreach(row =>{
      //获取数据时，可以通过索引获取
      println(row.get(1))
    })

    //RDD转换DS

    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    val rdd3: RDD[User] = userDS.rdd
    rdd3.foreach(
      //可以通过类的属性去访问
      row=>println(row.age)
    )

    spark.stop()


  }
}

//样例类

case class User(id: Int, name: String, age: Int)