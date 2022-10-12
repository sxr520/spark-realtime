package com.atguigu.gmall.realtime.util

import java.util
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.{XContent, XContentType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable.ListBuffer

/**
 * @author sxr
 * @create 2022-07-26-20:00
 *
 *ES进行读写操作
 */
object MyEsUtils {

  val esClient:RestHighLevelClient = build()

  def build() ={
    /**创建客户端对象**/
    val host : String = MyPropsUtils(MyConfig.ES_HOST)
    val port : String = MyPropsUtils(MyConfig.ES_PORT)
    val restClientBuilder: RestClientBuilder =
      RestClient.builder(new HttpHost(host, port.toInt))
    val esClient : RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    esClient
  }

  /**关闭ES对象*/
  def close(): Unit ={
    if(esClient != null ) esClient.close()
  }


  /**
   * 1. 批写
   * 2. 幂等写
   */
  def bulkSave(indexName:String, docs:List[(String, AnyRef)]): Unit ={
    val bulkRequest: BulkRequest = new BulkRequest(indexName)
    for (doc <- docs) {
      val indexRequest: IndexRequest = new IndexRequest()
      val docJson: String = JSON.toJSONString(doc._2,new SerializeConfig(true))
      indexRequest.source(docJson,XContentType.JSON)
      indexRequest.id(doc._1)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
  }


  /**
   * 增 -- 幂等 -- 指定id
   */




  /**
   * 查询指定的字段
   */

  /**
   * 查询指定的字段
   */
  def searchField(indexName: String, fieldName: String): List[String] = {
    //判断索引是否存在
    val getIndexRequest: GetIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean =
      esClient.indices().exists(getIndexRequest,RequestOptions.DEFAULT)
    if(!isExists){
      return null
    }
    //正常从ES中提取指定的字段
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest: SearchRequest = new SearchRequest(indexName)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName,null).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      esClient.search(searchRequest , RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap: util.Map[String, AnyRef] = hit.getSourceAsMap

      val mid: String = sourceMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }
//
//  def main(args: Array[String]): Unit = {
//   println( searchField("gmall_dau_info_2022-07-27","mid"))
//  }
//

}
