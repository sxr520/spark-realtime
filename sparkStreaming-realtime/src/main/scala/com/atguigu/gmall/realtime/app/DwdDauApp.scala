package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}
import com.atguigu.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.{lang, util}
import java.util.Date
import scala.collection.mutable.ListBuffer
/**
 * @author sxr
 * @create 2022-07-15-10:15
 *
 * 日活宽表
 *
 *  1.准备实时环境
 *  2.从redis读取偏移量
 *  3.从kafka消费数据
 *  4.提取偏移量结束点
 *  5.处理数据
 *    5.1 转换数据结构
 *    5.2 去重
 *    5.3 维度关联
 *  6.写入ES
 *  7.提交offsets
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //0.状态还原
    revertState()
    // 1.准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2.从redis读取offset
    val topicName: String = "DWD_PAGE_LOG_TOPIC"
    val groupId: String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

    //3. 从Kafka消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4.提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5.处理数据
      //5.1 转换数据结构
      val pageLogDStream = offsetRangesDStream.map(
        consumerRecord => {
          val data: String = consumerRecord.value()
          val pageLog = JSON.parseObject(data,classOf[PageLog])
          pageLog
        }
      )

    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd =>{
        println("自我审查前："+rdd.count())
      }
    )

    //5.2去重
    //自我审查：将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      PageLog => PageLog.last_page_id == null
    )
    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd =>{
        println("自我审查后："+rdd.count())
        println("------------------------")
      }
    )

    // 第三方审查：通过redis将当日活跃的mid维护起来，自我审查后的每条数据需要到redis中进行比对去重
    // redis 中如何维护日活状态
    // 类型 ：list set
    // key : DAU_DATE
    // value : mid的集合
    // 写入API ：lpush/rpush  sadd
    // 读取API ：lrange  smembers
    // 过期 ：24h
//    filterDStream.filter()  //每条数据都要执行一次，redis连接太频繁
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("第三方审查前：" + pageLogList.size)

        val jedisClient: Jedis = MyRedisUtils.getJedisClient
        val pageLogs: ListBuffer[PageLog] = new ListBuffer[PageLog]
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        for (pageLog <- pageLogList) {
          //提取每条数据的mid（我们的日活统计是基于mid的，也可以基于uid）
          val mid: String = pageLog.mid

          //获取日期，测试不同日期的数据，不能直接获取系统时间
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"

          //判断redis中是否包含
          /*
          以下代码在分布式环境中，存在并发问题，可能多个并行度会同时进入if中，导致最终同一个mid的数据多次保存          // list
          val mids: util.List[String] = jedisClient.lrange(redisDauKey, 0, -1)
          if(!mids.contains(mid)){
          jedisClient.lpush(redisDauKey,mid)
            pageLogs.append(pageLog)
          }
          //set
          val setMids: util.Set[String] = jedisClient.smembers(redisDauKey)
          if(!setMids.contains(mid)){
            jedisClient.sadd(redisDauKey,mid)
            pageLogs.append(pageLog)
          }
          */

          val isNew: lang.Long = jedisClient.sadd(redisDauKey, mid)
          if (isNew == 1L) { //sadd有返回结果 1就是第一次写返回的值，0就是已经存在，写不进去。实现了原子操作
            pageLogs.append(pageLog)
          }
        }
        jedisClient.close()
        println("第三方审查后：" + pageLogs.size)
        pageLogs.iterator
      }
    )
//    redisFilterDStream.print(1000)

    //5.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val jedisClient: Jedis = MyRedisUtils.getJedisClient
        val dauInfoes: ListBuffer[DauInfo] = ListBuffer[DauInfo]()

        val pageLogList: List[PageLog] = pageLogIter.toList

        for (pageLog <- pageLogList) {
          //1、将pageLog中的字段拷贝到DauInfo中
          val dauInfo: DauInfo = new DauInfo()
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //2.补充维度
          //2.1 用户信息维度
          val uid: String = pageLog.user_id
          val redisUidKey: String = "DIM:USER_INFO:" + uid
//          println("key + uid : "+redisUidKey)
          val userInfo: String = jedisClient.get(redisUidKey)
          val userInfoJSONObject: JSONObject = JSON.parseObject(userInfo)
          //提取性别
          val gender: String = userInfoJSONObject.getString("gender")
          //提取年龄
          //生日
          val birthday: String = userInfoJSONObject.getString("birthday")
          //换算年龄
          val birDay: LocalDate = LocalDate.parse(birthday)
          val nowDate: LocalDate = LocalDate.now()
          val period: Period = Period.between(birDay, nowDate)
          val age: Int = period.getYears
          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //2.2 地区信息维度
          val provinceId: String = dauInfo.province_id
          val redisProKey: String = "DIM:BASE_PROVINCE:" + provinceId
//          println("key + provinceId : "+redisProKey)
          val baseProvince: String = jedisClient.get(redisProKey)
          val BaseProvinceJsonObj: JSONObject = JSON.parseObject(baseProvince)
          val proName: String = BaseProvinceJsonObj.getString("name")
          val ProIsoCode: String = BaseProvinceJsonObj.getString("iso_code")
          val areaCode: String = BaseProvinceJsonObj.getString("area_code")
          val iso_3166_2 = BaseProvinceJsonObj.getString("iso_3166_2")

          dauInfo.province_area_code = areaCode
          dauInfo.province_iso_code = ProIsoCode
          dauInfo.province_3166_2 = iso_3166_2
          dauInfo.province_name = proName

          //2.3 日期字段处理
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArray = dtHr.split(" ")
          val dt = dtHrArray(0)
          val hr: String = dtHrArray(1).split(":")(0)

          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfoes.append(dauInfo)
        }
        jedisClient.close()
        dauInfoes.iterator
      }
    )
//    dauInfoDStream.print(1000)

    //6.写入到ES
    //按照天分割索引，通过模板索引控制
    //准备ES工具类
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = {
              dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            }
//            println("doc: "+docs)
            if (docs.nonEmpty) {
              // 索引名
              // 如果是真实的实时环境，直接获取当前日期即可.
              // 因为我们是模拟数据，会生成不同天的数据.
              // 从第一条数据中获取日期
              val head: (String, DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr: String = sdf.format(new Date(ts))
//              println("date: "+dateStr)
              val indexName: String = s"gmall_dau_info_$dateStr"
              println(indexName, head)
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        //提交offset
        MyOffsetUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 状态还原
   *
   * 在每次启动实时任务时， 进行一次状态还原。 以ES为准, 将所以的mid提取出来，覆盖到Redis中.
   */

  def revertState(): Unit ={
    //从ES中查询到所有的mid
    val date: LocalDate = LocalDate.now()
    val indexName : String = s"gmall_dau_info_$date"
    val fieldName : String = "mid"
    val mids: List[ String ] = MyEsUtils.searchField(indexName , fieldName)
    //删除redis中记录的状态（所有的mid）
    val jedis: Jedis = MyRedisUtils.getJedisClient
    val redisDauKey : String = s"DAU:$date"
    jedis.del(redisDauKey)
    //将从ES中查询到的mid覆盖到Redis中
    if(mids != null && mids.size > 0 ){
      /*for (mid <- mids) {
        jedis.sadd(redisDauKey , mid )
      }*/
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey , mid )  //不会直接到redis执行
      }

      pipeline.sync()  // 到redis执行
    }

    jedis.close()
  }

}
