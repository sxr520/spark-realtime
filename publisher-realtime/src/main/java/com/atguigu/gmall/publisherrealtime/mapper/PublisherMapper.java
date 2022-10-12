package com.atguigu.gmall.publisherrealtime.mapper;

import com.atguigu.gmall.publisherrealtime.bean.nameValue;

import java.util.List;
import java.util.Map;

/**
 * @author sxr
 * @create 2022-07-29-16:34
 */
public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<nameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize);
}
