package com.atguigu.gmall.publisherrealtime.service.impl;

import com.atguigu.gmall.publisherrealtime.bean.nameValue;
import com.atguigu.gmall.publisherrealtime.mapper.PublisherMapper;
import com.atguigu.gmall.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author sxr
 * @create 2022-07-29-16:37
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    PublisherMapper publisherMapper;

    /**
     * 业务详情
     */
    @Override
    public Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        //计算分页起始页
        Integer from  = (pageNo - 1) * pageSize;
        Map<String, Object> detailResults = publisherMapper.searchDetailByItem(date,itemName,from,pageSize);
        return detailResults;
    }



    /**
     *日活业务处理
     */
    @Override
    public Map<String, Object> doDauRealtime(String td) {
        Map<String, Object> dauResults =  publisherMapper.searchDau(td);
        return dauResults;
    }

    /**
     *交易分析业务处理
     */
    @Override
    public List<nameValue> doStatsByItem(String itemName, String date, String t) {
        List<nameValue> results = publisherMapper.searchStatsByItem(itemName,date,typeToField(t));
        return exchangeStatsByItem(results,t);
    }

    public String typeToField(String t){
        if (t.equals("age")){
            return "user_age";
        }else if (t.equals("gender")){
            return "user_gender";
        }else {
            return null;
        }
    }

    public List<nameValue> exchangeStatsByItem(List<nameValue> searchResults, String t){
        if("gender".equals(t)){
           if (searchResults.size()>0){
               for (nameValue searchResult : searchResults) {
                   String name = searchResult.getName();
                   if(name.equals("F")){
                       searchResult.setName("女");
                   }else if (name.equals("M")){
                       searchResult.setName("男");
                   }
               }
           }
           return searchResults;
        }else if ("age".equals(t)){
            double totalAmount20 = 0;
            double totalAmount20to30 = 0;
            double totalAmount30 = 0;
            if (searchResults.size() > 0){
                for (nameValue searchResult : searchResults) {
                    Integer age = Integer.parseInt(searchResult.getName());
                    Double value = Double.parseDouble(searchResult.getValue().toString());
                    if(age<20){
                        totalAmount20 += value;
                    }else if (age <30){
                        totalAmount20to30 += value;
                    }else if(age >= 30){
                        totalAmount30 += value;
                    }
                }
                searchResults.clear();
                searchResults.add(new nameValue("20岁以下",totalAmount20));
                searchResults.add(new nameValue("20岁到29",totalAmount20to30));
                searchResults.add(new nameValue("30岁以上",totalAmount30));
            }

            return searchResults;
        }else {
            return null;
        }
    }
}
