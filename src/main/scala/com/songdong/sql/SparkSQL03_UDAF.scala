package com.songdong.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_UDAF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //    val sc = new SparkContext(conf)
    //    val spark1: SparkSession = new SparkSession(sc,)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //引入隐式转换，增加函数
    //这里的spark是指sparksession对象的名字
    import spark.implicits._


    //自定义聚合函数

    //创建聚合函数对象
    val udaf = new MyAgeFunction
    //注册聚合函数
    spark.udf.register("avgAge",udaf)

    //使用聚合函数
    val frame: DataFrame = spark.read.json("/Users/jon/Desktop/学习笔记/test/employees.json")

    frame.createOrReplaceTempView("user")

    spark.sql("select avgAge(salary) from user").show()


    //释放资源
    spark.stop()


  }
}

//声明用户自定义聚合函数（弱类型）
class MyAgeFunction extends UserDefinedAggregateFunction {

  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("salary", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("salary", LongType).add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果更新缓冲区的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}