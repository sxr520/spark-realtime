package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

/**
 * @author sxr
 * @create 2022-05-31-21:25
 *
 * 1.准备实时处理环境StreamingContext
 *
 * 2.从kafka消费数据
 *
 * 3.处理数据
 *    3.1转换数据结构
 *        专用结构 Bean
 *        通用结构 Map JsonObject
 *
 *    3.2分流 ：将数据拆分到不同的主题中
 *    启动主题: DWD_START_LOG
 *    页面访问主题: DWD_PAGE_LOG
 *    页面动作主题:DWD_PAGE_ACTION
 *    页面曝光主题:DWD_PAGE_DISPLAY
 *    错误主题:DWD_ERROR_INFO
 *
 * 4.写到DWD层
 *
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    val conf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


    //2.从kafka消费数据
    val topicName: String = "ODS_BASE_LOG_1018"
    val groupId: String = "ODS_BASE_LOG_GROUP"

    // TODO   从redis中读取offset，指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)
    var KafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets!=null && offsets.nonEmpty){
      //指定offset进行消费
       KafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId,offsets)
    }else{
      //默认的offset进行消费
      KafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // TODO 补充：从当前消费到的数据中提取offset，不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = KafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

//    3.处理数据
//       3.1转换数据结构
val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map (
  ConsumerRecord => {
    val log: String = ConsumerRecord.value()
    val jSONObject: JSONObject = JSON.parseObject(log)
    jSONObject
  }
)
//   jsonObjDStream.print(1000)

    ///3.2分流 ：将数据拆分到不同的主题中
    val DWD_PAGE_LOG_TOPIC :String = "DWD_PAGE_LOG_TOPIC"          //页面访问
    val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC"        //错误数据
    val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC"        //启动数据
    val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
    val DWD_PAGE_ACTION_TOPIC :String = "DWD_PAGE_ACTION_TOPIC"     //页面事件

    //分流规则：
    //  错误数据：不做任何的拆分，只要包含错误字段，直接整条数据发送到对应的topic
    //  页面数据：拆分到页面访问，曝光，事件
    //  启动数据：发动到对应的topic

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          JSONObject=>{
            //分流过程
            //分流错误数据
            val errObj: JSONObject = JSONObject.getJSONObject("err")
            if (errObj !=null){
              //将该错误数据发送到DWD_ERROR_LOG_TOPIC
              MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC,JSONObject.toJSONString)
            }else{
              //提取公共字段
              val commontObj: JSONObject = JSONObject.getJSONObject("common")
              val ar: String = commontObj.getString("ar")
              val uid: String = commontObj.getString("uid")
              val os: String = commontObj.getString("os")
              val ch: String = commontObj.getString("ch")
              val is_new: String = commontObj.getString("is_new")
              val md: String = commontObj.getString("md")
              val mid: String = commontObj.getString("mid")
              val vc: String = commontObj.getString("vc")
              val ba: String = commontObj.getString("ba")
              val ts: Long = commontObj.getLong("ts") //时间戳

              //页面数据
              val pageObj: JSONObject = JSONObject.getJSONObject("page")
              if(pageObj!=null){
                //提取page字段
                val page_id: String = pageObj.getString("page_id")
                val item: String = pageObj.getString("item")
                val during_time: Long = pageObj.getLong("during_time")
                val item_type: String = pageObj.getString("item_type")
                val last_page_id: String = pageObj.getString("last_page_id")
                val source_type: String = pageObj.getString("source_type")

                //封装到pageLog
                val pageLog: PageLog = PageLog(mid,uid,ar,ch,is_new,md,os,vc,ba,page_id,last_page_id,item,item_type,during_time,source_type,ts)

                //发送到 DWD_PAGE_LOG_TOPIC
                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC,JSON.toJSONString(pageLog,new SerializeConfig(true)))
                        //JSON.toJSONString()是java的方法，会在里面找get(), set()方法，这是scala所以加一个SerializeConfig去找字段，不找get,set方法

                //提取曝光字段
                val displaysJsonArr = JSONObject.getJSONArray("displays")
                if(displaysJsonArr!=null && displaysJsonArr.size()>0 ){
                  for (i <- 0 until displaysJsonArr.size() ){
                    val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                    val display_type: String = displayObj.getString("display_type")
                    val item: String = displayObj.getString("item")
                    val item_type: String = displayObj.getString("item_type")
                    val pos_id: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")

                    //封装到对象
                    val displayLog: PageDisplayLog = PageDisplayLog(
                      mid, uid, ar, ch, is_new, md, os, vc, ba, page_id, last_page_id,
                      item, item_type, during_time, source_type, display_type, item, item_type, order, pos_id, ts
                    )

                    //写到 DWD_PAGE_DISPLAY_TOPIC
                    MyKafkaUtils.send(
                      DWD_PAGE_DISPLAY_TOPIC,
                      JSON.toJSONString(displayLog,new SerializeConfig(true))
                    )

                  }
                }

                //提取事件字段
                val actionsJsonArr = JSONObject.getJSONArray("actions")
                if(actionsJsonArr!=null && actionsJsonArr.size()>0 ) {
                  for (i <- 0 until actionsJsonArr.size()) {
                    val actionObj: JSONObject = actionsJsonArr.getJSONObject(i)
                    val item: String = actionObj.getString("item")
                    val action_id: String = actionObj.getString("action_id")
                    val item_type: String = actionObj.getString("item_type")
                    val ts: Long = actionObj.getLong("ts")

                    //封装成pageActionLog对象
                    val pageActionLog: PageActionLog = PageActionLog(
                      mid, uid, ar, ch, is_new, md, os, vc, ba, page_id, last_page_id,
                      item, item_type, during_time, source_type, action_id, item, item_type, ts
                    )

                    //发送到 DWD_PAGE_ACTION_TOPIC
                    MyKafkaUtils.send(
                      DWD_PAGE_ACTION_TOPIC,
                      JSON.toJSONString(pageActionLog, new SerializeConfig(true))
                    )
                  }
                }
              }

              //启动数据
              val startObj: JSONObject = JSONObject.getJSONObject("start")
              if(startObj!=null){
                val entry: String = startObj.getString("entry")
                val open_ad_skip_ms: Long = startObj.getLong("open_ad_skip_ms")
                val open_ad_ms: Long = startObj.getLong("open_ad_ms")
                val loading_time: Long = startObj.getLong("loading_time")
                val open_ad_id: String = startObj.getString("open_ad_id")

                //封装到对象
                val startLog: StartLog = StartLog(mid, uid, ar, ch, is_new, md, os, vc, ba, entry, open_ad_id, loading_time, open_ad_ms, open_ad_skip_ms, ts)
                // 发送到 DWD_START_LOG_TOPIC
                MyKafkaUtils.send(
                  DWD_START_LOG_TOPIC,
                  JSON.toJSONString(startLog,new SerializeConfig(true))
                )
              }

            }
            //C : 算子(foreach)里面 : Executor端执行, 每批次每条数据执行一次.
          }
        )
        //B: foreachRDD里面， 算子(foreach)外面:  Driver端执行. 每批次执行一次.
        MyOffsetUtils.saveOffset(topicName,groupId,offsetRanges)
      }
    )
    //A: foreachRDD外面: Driver端执行. 程序启动的时候执行一次。

    ssc.start()
    ssc.awaitTermination()

  }
}
