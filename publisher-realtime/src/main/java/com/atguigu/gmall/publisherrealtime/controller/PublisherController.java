package com.atguigu.gmall.publisherrealtime.controller;

import com.atguigu.gmall.publisherrealtime.bean.nameValue;
import com.atguigu.gmall.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.Mapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author sxr
 * @create 2022-07-29-16:33
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;


    /**
     *明细列表
     *
     * http://bigdata.gmall.com/detailByItem?date=2021-02-02&itemName=小米手机&pageNo=1&pageSize=20
     */

    @GetMapping("detailByItem")
    public Map<String,Object> detailByItem(
            @RequestParam("date") String date,
            @RequestParam("itemName") String itemName,
            @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo,
            @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize){
        Map<String,Object> results = publisherService.doDetailByItem(date,itemName,pageNo,pageSize);
        return results;
    }

    /**
     * 交易分析，按照类别（年龄，性别）统计
     *
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     */
    @GetMapping("statsByItem")
    public List<nameValue> statsByItem(
            @RequestParam("itemName") String itemName,
            @RequestParam("date") String date,
            @RequestParam("t") String t){
       List<nameValue> results =  publisherService.doStatsByItem(itemName,date,t);
       return results;
    }


    /**
     * 日活分析
     */
    @GetMapping("dauRealtime")
    public Map<String,Object> dauRealtime(@RequestParam("td") String td){

        //http://bigdata.gmall.com/dauRealtime?td=2022-07-27

        Map<String,Object>  results = publisherService.doDauRealtime(td);

        return results;
    }
}
