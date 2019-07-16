package com.songdong.kafkaStreaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //kafka的参数
    var brokers = "192.168.1.111:9092,192.168.1.112:9092"
    val zookeeper = "192.168.1.111:2181,192.168.1.112:2181"
    val sourceTopic = "source"
    val targetTopic = "target"
    val consumerGroup = "consumer0716"
    //封装Kafka参数
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,

      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.codehaus.jackson.map.deser.std.StringDeserializer",

      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.codehaus.jackson.map.deser.std.StringDeserializer",

      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup
    )

    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(sourceTopic))

    kafkaDStream.foreachRDD { rdd =>
      rdd.foreachPartition { rddPar =>

        //创建生产者
        val kafkaPool = KafkaPool(brokers)
        val kafkaConn = kafkaPool.borrowObject()
        //写出到kafka（targetTopic）
        for (item <- rddPar) {
          //生产者发送数据
          kafkaConn.send(targetTopic, item._2)
        }
        //关闭生产者
        kafkaPool.returnObject(kafkaConn)
      }
    }

    //test
    //    val result = kafkaStream.map(x => (x._1, x._2)).reduceByKey(_ + _)
    //    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
