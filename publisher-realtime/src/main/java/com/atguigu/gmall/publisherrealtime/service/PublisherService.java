package com.atguigu.gmall.publisherrealtime.service;

import com.atguigu.gmall.publisherrealtime.bean.nameValue;

import java.util.List;
import java.util.Map;

/**
 * @author sxr
 * @create 2022-07-29-16:33
 */

public interface PublisherService {
    Map<String, Object> doDauRealtime(String td);

    List<nameValue> doStatsByItem(String itemName, String date, String t);

    Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);
}
