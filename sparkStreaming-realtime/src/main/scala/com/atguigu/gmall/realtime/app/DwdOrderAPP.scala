package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.util.{MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/**
 *  1.准备实时环境
 *  2.从redis读取偏移量 * 2
 *  3.从kafka消费数据  * 2
 *  4.提取偏移量结束点/offset * 2
 *  5.处理数据
 *    5.0 转换结构
 *    5.1 维度关联
 *    5.2 双流join
 *  6.写入ES
 *  7.提交offsets
 */
object DwdOrderAPP {
  def main(args: Array[String]): Unit = {
    // 1.准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //2.从redis读取偏移量
    //order_info
    val orderInfoTopicName :String = "DWD_ORDER_INFO_I"
    val orderInfoGroup : String = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderInfoTopicName, orderInfoGroup)
    //order_detail
    val orderDetailTopicName : String = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup : String = "DWD_ORDER_DETAIL:GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    //3.从kafka消费数据
    //order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets !=null && orderInfoOffsets.nonEmpty){
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup,orderInfoOffsets)
    }else{
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }

    //order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets !=null && orderInfoOffsets.nonEmpty){
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup,orderDetailOffsets)
    }else{
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup)
    }

    // 4.提取偏移量结束点/offset
    //order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetRangesDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //order_detail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetRangesDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.处理数据
    //5.0 转换结构
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
//    orderInfoDStream.print(100)

    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )

//    orderDetailDStream.print(100)

    //5.1 维度关联
    //order_info
    val orderInfoDIMDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        val orderInfos: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val jedisClient: Jedis = MyRedisUtils.getJedisClient
        for (orderInfo <- orderInfoIter) {
          //关联用户维度
          val user_id: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$user_id"
          val userInfoJson: String = jedisClient.get(redisUserKey)
          val userInfoObj: JSONObject = JSON.parseObject(userInfoJson)
          val userGender: String = userInfoObj.getString("gender")
          val birthday: String = userInfoObj.getString("birthday")
          //换算年龄
          val birthDate: LocalDate = LocalDate.parse(birthday)
          val nowDate: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthDate, nowDate)
          val age: Int = period.getYears
          //填充到对象
          orderInfo.user_gender = userGender
          orderInfo.user_age = age

          //关联地区维度
          val province_id: Long = orderInfo.province_id
          val redisProKey: String = s"DIM:BASE_PROVINCE:$province_id"
          val baseProvince: String = jedisClient.get(redisProKey)
          val baseProvinceObj: JSONObject = JSON.parseObject(baseProvince)
          val proName: String = baseProvinceObj.getString("name")
          val proIsoCode: String = baseProvinceObj.getString("iso_code")
          val proAreaCode: String = baseProvinceObj.getString("area_code")
          //补充到对象
          orderInfo.province_name = proName
          orderInfo.province_iso_code = proIsoCode
          orderInfo.province_area_code = proAreaCode

          //处理日期字段 2020-03-10 00:00:00
          val create_time: String = orderInfo.create_time
          val create_date: String = create_time.split(" ")(0)
          val create_hour: String = create_time.split(" ")(1).split(":")(0)
          //补充到对象
          orderInfo.create_date = create_date
          orderInfo.create_hour = create_hour

          orderInfos.append(orderInfo)

        }
        jedisClient.close()
        orderInfos.iterator
      }
    )
//    orderInfoDIMDStream.print(100)

    // 5.2双流join
    //内连接
    //外连接
      //左外连
      //右外连
      //全连接

    val orderInfoKvDStream: DStream[(Long, OrderInfo)] =
      orderInfoDIMDStream.map(orderInfo => (orderInfo.id, orderInfo))

    val orderDetailKvDStream: DStream[(Long, OrderDetail)] =
      orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    // 会出现延迟
//    val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKvDStream.join(orderDetailKvDStream)
    //解决延迟
    // 1. 扩大采集周期，治标不治本
    // 2. 首先采用fullOutJoin,保证没有join成功的数据保留下来
    //      让双方都多两步操作，到缓存中找到对的人，把自己写到缓存中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoKvDStream.fullOuterJoin(orderDetailKvDStream)

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedisClient: Jedis = MyRedisUtils.getJedisClient
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          //orderInfo有，orderDetail有
          if (orderInfoOp.isDefined) {
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              val orderDetail: OrderDetail = orderDetailOp.get
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            }
            //orderInfo有，orderDetail没有

            //orderInfo写缓存
            // 类型: String
            // key: ORDERJOIN:ORDER_INFO:ID
            // value: json
            // 写入API: set
            // 读取API: get
            // 是否过期: 24小时

            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            //            jedisClient.set(redisOrderInfoKey,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            //            jedisClient.expire(redisOrderInfoKey,24*3600)
            jedisClient.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

            //orderInfo读缓存
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedisClient.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
                orderWides.append(orderWide)

              }
            }

          } else {
            //orderInfo没有，orderDetail有
            val orderDetail: OrderDetail = orderDetailOp.get
            //读缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedisClient.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.isEmpty) {
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)

            }

            //写缓存
            // 类型: set
            // key: ORDERJOIN:ORDER_DETAIL:ORDER_ID
            // value: json ,json,json..
            // 写入API: sadd
            // 读取API: smembers
            // 是否过期: 24小时
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
            jedisClient.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail,new SerializeConfig(true)))
            jedisClient.expire(redisOrderDetailKey, 24 * 3600)

          }
        }
        jedisClient.close()
        orderWides.toIterator
      }
    )

//    orderWideDStream.print(100)

//    orderJoinDStream.print(100)

    //写入ES
    orderWideDStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          orderInfoIter=>{
            val orders=
                orderInfoIter.map((orderInfo => (orderInfo.detail_id.toString, orderInfo))).toList

            if(orders.nonEmpty){
                val head= orders.head
                val dateStr = head._2.create_date
                val indexName : String = s"gmall_order_wide_$dateStr"
              println(indexName,orders)
                MyEsUtils.bulkSave(indexName,orders)

            }

          }
        )
        //提交offset
        MyOffsetUtils.saveOffset(orderInfoTopicName,orderInfoGroup,orderInfoOffsetRanges)
        MyOffsetUtils.saveOffset(orderDetailTopicName,orderDetailGroup,orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
