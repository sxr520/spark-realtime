package com.atguigu.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis
import java.util
import scala.collection.mutable


/**
 * @author sxr
 * @create 2022-06-04-19:41
 *
 * Offset管理工具类 用于redis往中存储和读取offset
 *
 *  管理方案:
 *     1.  后置提交偏移量  ->  手动控制偏移量提交
 *     2.  手动控制偏移量提交 ->  SparkStreaming提供了手动提交方案，但是我们不能用，因为我们会对DStream的结构进行转换.
 *     3.  手动的提取偏移量维护到redis中
 *         -> 从kafka中消费到数据,先提取偏移量
 *         -> 等数据成功写出后，将偏移量存储到redis中
 *         -> 从kafka中消费数据之前，先到redis中读取偏移量， 使用读取到的偏移量到kafka中消费数据
 *
 *     4. 手动的将偏移量存储到redis中，每次消费数据需要使用存储的offset进行消费，每次消费数据后，要将本次消费的offset存储到redis中。
 *
 */
object MyOffsetUtils {

  /**
   * 往Redis中存储offset
   * 问题：存的offset从哪里来？
   *      从消费的数据中提取出来的，传入到该方法中
   *      offsetRanges: Array[OffsetRange]
   *    offset的结构是什么？
   *       Kafka中offset维护的结构
   *       groupID + topic + partition => offset
   *       从传入的offset中提取信息
   *    在redis中怎么存？
   *       类型 : hash
   *       key : groupID + topic
   *       value : partition - offset
   *       写入API : hset/hmset
   *       读取API　: hgetall
   *       是否过期 : 不过期
   */
  def saveOffset(topic : String, groupId : String, offsetRanges : Array[OffsetRange]): Unit ={
    if(offsetRanges!=null && offsetRanges.length>0){
      val offsets: util.HashMap[String, String] = new util.HashMap[String, String]()

      for (offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val offset: Long = offsetRange.untilOffset
        offsets.put(partition.toString,offset.toString)
      }

      // 往redis中存
      val jedis: Jedis = MyRedisUtils.getJedisClient
      val redisKey:String  = s"offsets:$topic:$groupId"
      jedis.hset(redisKey,offsets)
      jedis.close()
      println("提交offset:"+offsets)
    }
  }


  /**
   * 从Redis中读取存储的offset
   *
   * 问题：
   *      如何让SparkStreaming通过指定的offset进行消费？
   *
   *      SparkStreaming要求的offset格式是什么？
   *      Map[TopicPartition,Long]
   */

  def readOffset(topic:String,groupId :String):Map[TopicPartition,Long]={
    val jedis: Jedis = MyRedisUtils.getJedisClient
    val redisKey:String  = s"offsets:$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取到offset:"+offsets)

    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    if (offsets!=null && offsets.size() > 0) {
      //将java的map转化为Scala的map进行迭代
      import scala.collection.JavaConverters._
      for ((partition, offset) <- offsets.asScala) {
        val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
        results.put(topicPartition, offset.toLong)
      }
    }
    jedis.close()
    results.toMap
  }

}
