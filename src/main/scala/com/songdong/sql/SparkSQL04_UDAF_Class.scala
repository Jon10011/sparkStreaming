package com.songdong.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

object SparkSQL04_UDAF_Class {
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
    val udaf = new MyAvgSalaryFunction

    //将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgSalary")
    
    val frame: DataFrame = spark.read.json("/Users/jon/Desktop/学习笔记/test/employees.json")
    
    val userDS: Dataset[UserBean] = frame.as[UserBean]

    //应用函数
    userDS.select(avgCol).show()
    
    
    //释放资源
    spark.stop()


  }
}

case class UserBean(name: String, salary: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

//声明用户自定义聚合函数(强类型)
//继承Aggregator，设定范型
//实现方法
class MyAvgSalaryFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.salary
    b.count = b.count + 1
    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}