package com.atguigu.gmall.publisherrealtime.mapper.impl;


import com.atguigu.gmall.publisherrealtime.bean.nameValue;
import com.atguigu.gmall.publisherrealtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.lang.management.PlatformLoggingMXBean;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sxr
 * @create 2022-07-29-16:37
 */
@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

//    @Resource
    @Autowired
    RestHighLevelClient esClient;

    private String dauIndexNamePrefix = "gmall_dau_info_";

    private String orderIndexNamePrefix = "gmall_order_wide_";


    /**
     * 业务详情查询
     */
    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize) {
        HashMap<String, Object> results = new HashMap<>();
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("sku_name", itemName);
        matchQuery.operator(Operator.AND);
        searchSourceBuilder.query(matchQuery);
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.fetchSource(new String[]{"create_time","order_price","province_name","sku_name","sku_num","split_total_amount","user_age","user_gender"},null);
        searchSourceBuilder.highlighter(new HighlightBuilder().field("sku_name"));

        searchRequest.source(searchSourceBuilder);


        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long total = searchResponse.getHits().getTotalHits().value;
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            ArrayList<Map<String, Object>> maps = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                //提取source
                Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                //将key final_total_amount 修改为 total_amount
                Object total_amount = sourceMap.get("split_total_amount");
                sourceMap.remove("split_total_amount");
                sourceMap.put("total_amount",total_amount);
                //提取高亮
                Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("sku_name");
                Text[] fragments = highlightField.getFragments();
                String highLightSkuName = fragments[0].toString();
                //将高亮覆盖原结果
                sourceMap.put("sku_name",highLightSkuName);

                maps.add(sourceMap);
            }
            //处理最终结果
            results.put("total",total);
            results.put("detail",maps);

            return results;


        } catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND){
                log.warn(indexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询ES失败");
        }
        return results;
    }



    /**
     * 交易分析业务处理
     *
     * @param field  按性别还是年龄统计 age gender
     */
    @Override
    public List<nameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<nameValue> results = new ArrayList<>();

        String orderIndexName = orderIndexNamePrefix + date;

        SearchRequest searchRequest = new SearchRequest(orderIndexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //不需要明细
        sourceBuilder.size(0);
        //query
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("sku_name",itemName);
        matchQuery.operator(Operator.AND);
        sourceBuilder.query(matchQuery);
        //aggs
        TermsAggregationBuilder terms = null;
        if(field.equals("user_gender")){
            terms = AggregationBuilders
                    .terms("groupby"+field)
                    .field(field+".keyword")
                    .size(100);
        }else {
            terms = AggregationBuilders
                    .terms("groupby"+field)
                    .field(field)
                    .size(100);
        }

        //sum
        SumAggregationBuilder sumAggregationBuilder =
                AggregationBuilders.sum("totalamount").field("split_total_amount");
        terms.subAggregation(sumAggregationBuilder);
        sourceBuilder.aggregation(terms);

        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupby"+field);
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String key = bucket.getKeyAsString();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum totalmount = bucketAggregations.get("totalamount");
                double value = totalmount.getValue();
                results.add(new nameValue(key,value));
            }
            return results;

        } catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND){
                log.warn(orderIndexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询ES失败");
        }

        return results;
    }



    /**
     *日活业务处理
     */    @Override
    public Map<String, Object> searchDau(String td) {

        HashMap<String, Object> dauResults = new HashMap<>();

        Long dauTotal = searchDauTotal(td);

        dauResults.put("dauTotal",dauTotal);


        //今天分时明细
        Map<String, Long> dauTd = searchDauHr(td);
        dauResults.put("dauTd",dauTd);

        //昨日分时明细
        LocalDate tdld = LocalDate.parse(td);
        LocalDate ydld = tdld.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(ydld.toString());
        dauResults.put("dauYd",dauYd);

        return dauResults;
    }

    public Long searchDauTotal(String td){
        String indexName = dauIndexNamePrefix + td;

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotal = searchResponse.getHits().getTotalHits().value;
            return dauTotal;
        }catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND){
                log.warn(indexName + "不存在...");
                return 0L;
            }
        }catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询ES失败");
        }
        return 0L;
    }

    public Map<String,Long> searchDauHr(String td){
        HashMap<String, Long> dauHr = new HashMap<>();
        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //不要明细
        sourceBuilder.size(0);
        //聚合
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupByHr")
                .field("hr")
                .size(24);
        sourceBuilder.aggregation(aggregationBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupByHr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long hrTotal = bucket.getDocCount();
                dauHr.put(hr,hrTotal);
            }

            return dauHr;

        }catch (ElasticsearchStatusException ese){
            if (ese.status() == RestStatus.NOT_FOUND){
                log.warn(indexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("查询ES失败");
        }
        return dauHr;

    }

}
