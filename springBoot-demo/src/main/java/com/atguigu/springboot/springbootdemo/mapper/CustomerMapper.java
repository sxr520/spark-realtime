package com.atguigu.springboot.springbootdemo.mapper;

import com.atguigu.springboot.springbootdemo.bean.Customer;
import com.atguigu.springboot.springbootdemo.mapper.impl.CustomerMapperImpl;

/**
 * @author sxr
 * @create 2022-07-29-15:31
 */
public interface CustomerMapper   {
    Customer searchByUsernameAndPassword(String username, String password);
}
