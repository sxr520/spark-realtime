package com.atguigu.gmall.realtime.util


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 * @author sxr
 * @create 2022-05-26-22:42
 */

/*
 * Kafka 工具类，用于生产和消费
 */
object MyKafkaUtils {

//消费者配置ConsumerConfig
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    //kafka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    //kv反序列化器 deserializer
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //groupId

    //offset的提交 自动 手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG ->"true",
//    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG

    //offset的重置 默认:"latest", "earliest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"//latest

    //....
  )

  /**
   * 消费
   */
  def getKafkaDStream(ssc:StreamingContext,topic:String,groupId:String)={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)

    val KafkaDStream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
    KafkaDStream
  }
  //使用指定的offset进行消费
  def getKafkaDStream(ssc:StreamingContext,topic:String,groupId:String,offsets:Map[TopicPartition, Long])={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)

    val KafkaDStream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs,offsets)
    )
    KafkaDStream
  }


  /**
   * 生产
   */
  val producer : KafkaProducer[String,String]=createProducer()

  def createProducer(): KafkaProducer[String,String] = {
    //生产者配置ProducerConfig
    val producerConfigs = new util.HashMap[String, AnyRef]

    //kafka集群位置
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG ,MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))

    //kv序列化器 serializer
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    //ack
    producerConfigs.put(ProducerConfig.ACKS_CONFIG,"all")
    //batch.size
    //linger.ms
    //retries
    //幂等性
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

    val producer = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  //（按照默认的粘性分区策略）
  def send(topic:String,msg:String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,msg))
  }

  //（按照key进行分区）
  def send(topic:String,key:String,msg:String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,key,msg))
  }

  /**
   * 关闭生产对象
   */
  def close(): Unit ={
    if(producer !=null){
      producer.close()
    }
  }

  /**
   * 刷写，将缓冲区的数据刷写到磁盘
   */
  def flush(): Unit ={
    producer.flush()
  }

}


