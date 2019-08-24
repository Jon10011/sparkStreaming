package com.songdong.streaming.kafkaStreaming

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


class KafkaProxy(brokers: String) {

  //存放配置文件
  private val pros: Properties = new Properties()
  pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val kafkaConn = new KafkaProducer[String, String](pros)

  //封装发送的方法
  def send(topic: String, key: String, value: String): Unit = {
    kafkaConn.send(new ProducerRecord[String, String](topic, key, value))
  }

  //重载send
  def send(topic: String, value: String): Unit = {
    kafkaConn.send(new ProducerRecord[String, String](topic, value))
  }

  def close: Unit = {
    kafkaConn.close()
  }
}


//工厂
class KafkaProxyFactory(brokers: String) extends BasePooledObjectFactory[KafkaProxy] {

  //创建实例
  override def create(): KafkaProxy = new KafkaProxy(brokers)

  //将池中对象封装
  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}

object KafkaPool{
  //声明一个连接池对象
  var kafkaPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers:String): GenericObjectPool[KafkaProxy] = {
    if (kafkaPool == null) {
      kafkaPool.synchronized {
        if (kafkaPool == null) {
          kafkaPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
        }
      }
    }
    kafkaPool
  }

}
