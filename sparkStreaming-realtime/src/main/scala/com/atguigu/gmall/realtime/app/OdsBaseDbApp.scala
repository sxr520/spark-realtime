package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

/**
 * @author sxr
 * @create 2022-07-13-10:08
 *
 * 业务数据消费分流
 *
 * 1. 准备实时环境
 * 2. 从redis读取偏移量
 * 3. 从Kafka消费数据
 * 4. 提取偏移量结束点
 * 5. 数据处理
 *     5.1 转换数据结构
 *     5.2 分流
 *          事实数据 => kafka
 *          维度数据 => redis
 * 6. flush kafka的缓冲区
 * 7. 提交offset
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    val conf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topicName: String = "ODS_BASE_DB_M"
    val groupId: String = "ODS_BASE_DB_GROUP"

    //2. 从redis读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

    //3. 从Kafka消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets!=null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5. 数据处理
        //5.1 转换数据结构
        val JsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
          consumerRecord => {
            val jsonData: String = consumerRecord.value()
            val jSONObject: JSONObject = JSON.parseObject(jsonData)
            jSONObject
          }
        )
        JsonObjDStream.print(100)
      //5.2 分流

//      //事实表清单
//      val factTables :Array[String] = Array[String]("order_info","order_detail")
//      //维度表清单
//      val dimTables :Array[String] = Array[String]("user_info","base_province")



      JsonObjDStream.foreachRDD(
        rdd =>{
          // Driver
          //TODO 如何动态维护表清单?
          // 将表清单维护到redis中,代码中只要保证能够周期性的读取redis中维护的表清单.
          // Redis中如何存储:
          // type:     set
          // key :     FACT:TABLES   DIM:TABLES
          // value:   表名的集合
          // 写入API:  sadd
          // 读取API:  smembers
          // 是否过期: 不过期
          val redisFactKeys :String = "FACT:TABLES"
          val redisDimKeys : String = "DIM:TABLES"
          val jedisClient : Jedis = MyRedisUtils.getJedisClient
          //事实表清单
          val factTables: util.Set[String] = jedisClient.smembers(redisFactKeys)
          val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)

          println("factTables: " + factTables)

          //维度表清单
          val dimTables  = jedisClient.smembers(redisDimKeys)
          val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
          println("dimTables: " + dimTables)

          jedisClient.close()

          rdd.foreachPartition(
            jsonObjIter =>{
              val jedisClient: Jedis = MyRedisUtils.getJedisClient
              for (jsonObj <- jsonObjIter) {
                //提取操作类型
                val operType: String = jsonObj.getString("type")
                val opValue: String = operType match {
                  case "bootstrap-insert" => "I"
                  case "insert" => "I"
                  case "update" => "U"
                  case "delete" => "D"
                  case _ => null
                }
                //判断操作类型：1.明确什么操作 2.过滤不感兴趣的操作
                if (opValue != null){
                  // 提取表名
                  val tableName: String = jsonObj.getString("table")
                  // 提取数据
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  if(factTablesBC.value.contains(tableName)){
                    println(dataObj.toJSONString)
                    //事实数据
                    val dwdTopicName:String = s"DWD_${tableName.toUpperCase}_${opValue}"
                    MyKafkaUtils.send(dwdTopicName,dataObj.toJSONString)
                  }
                  if(dimTablesBC.value.contains(tableName)){
                    //维度数据
                    /*
                    *       类型 : string
                    *       key : DIM:表名:ID
                    *       value : 整条数据的jsonString
                    *       写入API : set
                    *       读取API　: get
                    *       是否过期 : 不过期
                    */
                    val id: String = dataObj.getString("id")
                    val redisKey :String = s"DIM:${tableName.toUpperCase()}:$id"
                    // TODO 此处获取Redis连接好不好?
                    //不好， 每条数据都要获取一次连接， 用完直接还回去.
                    //我们希望获取一次连接，反复使用，最终还回去.
//                    val jedisClient: Jedis = MyRedisUtils.getJedisClient
                    jedisClient.set(redisKey,dataObj.toJSONString)

                  }
                }

              }

              //关闭redis连接
              jedisClient.close()
              //刷新kafka缓冲区
              MyKafkaUtils.flush()
            }

          )
          //提交offset
          MyOffsetUtils.saveOffset(topicName,groupId,offsetRanges)
        }
      )


    ssc.start()
    ssc.awaitTermination()
  }
}
