package com.atguigu.springboot.springbootdemo.mapper.impl;

import com.atguigu.springboot.springbootdemo.bean.Customer;
import com.atguigu.springboot.springbootdemo.mapper.CustomerMapper;
import org.springframework.stereotype.Repository;

/**
 * @author sxr
 * @create 2022-07-29-15:33
 */
@Repository
public class CustomerMapperImpl implements CustomerMapper {

    @Override
    public Customer searchByUsernameAndPassword(String username, String password) {
        System.out.println("CustomerMapperImpl: 数据库查询操作！");
        //JDBC
        //...
        return new Customer(username,password,null,null,null);
    }
}
