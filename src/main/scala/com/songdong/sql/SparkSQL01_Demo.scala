package com.songdong.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //    val sc = new SparkContext(conf)
    //    val spark1: SparkSession = new SparkSession(sc,)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val frame: DataFrame = spark.read.json("/Users/jon/Desktop/学习笔记/test/employees.json")

    frame.createOrReplaceTempView("emp1")

    spark.sql("select * from emp1").show()


    spark.stop()


  }
}
