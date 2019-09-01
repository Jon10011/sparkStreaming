package com.songdong.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val sc = new SparkContext(conf)

    val tuples: Array[(String, Int)] = Array(("a",1),("b",2),("c",3),("b",4),("c",6),("b",9),("c",1))

    val value: RDD[(String, Int)] = sc.parallelize(tuples)
    val map = new mutable.HashMap[String,Int]()




    val MAPRDD = value.collect().map(x => {
      sc
      map += x._1 -> x._2
    })

value.foreachPartition(it => {
  var i = 0;

  it.foreach(
    x=>{
      i = i + 1
    }
  )
})




//    val edf: RDD[((String, String), Array[String])] = edf
//      .map(x => (x._1, x._2.toString))
//      .reduceByKey(_ + ":" + _)
//      .map(x => (x._1, x._2.split(":").sortBy(x=>x)))




//    val value1: RDD[(String, Iterable[Int])] = edf.groupByKey()


//    val res: RDD[Array[Int]] = value1.map(it => {
//      it._2.toArray.sortBy(x => x).reverse.take(2)
//    })

//    val res: RDD[(String, List[Int])] = value1.mapValues(it => {
//      it.toList.sortBy(x => x).reverse.take(2)
//    })
//    val buffer: mutable.Buffer[(String, List[Int])] = res.collect().toBuffer
//    println(buffer)

//    res.t
//    val buffer: mutable.Buffer[Array[Int]] = res.collect.toBuffer
//    for(t <- buffer){
//      println(t.mkString(","))
//    }

//    val array: Array[Array[Int]] = res.collect()
//    println(array.toBuffer)


    //    val spark1: SparkSession = new SparkSession(sc,)

//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//
//    val frame: DataFrame = spark.read.json("/Users/jon/Desktop/学习笔记/test/employees.json")
//
//    frame.createOrReplaceTempView("emp1")
//
//    spark.sql("select * from emp1").show()
//
//
//    spark.stop()
    sc.stop()


  }
}
