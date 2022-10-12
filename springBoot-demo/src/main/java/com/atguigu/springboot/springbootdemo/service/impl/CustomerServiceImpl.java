package com.atguigu.springboot.springbootdemo.service.impl;

import com.atguigu.springboot.springbootdemo.bean.Customer;
import com.atguigu.springboot.springbootdemo.mapper.CustomerMapper;
import com.atguigu.springboot.springbootdemo.service.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author sxr
 * @create 2022-07-28-17:47
 */

@Service //表示成业务层
public class CustomerServiceImpl implements CustomerService {

    @Autowired
    CustomerMapper customerMapper;

    public String doLogin(String username,String password){

        System.out.println("复杂的业务处理");

        Customer customer = customerMapper.searchByUsernameAndPassword(username, password);
        if (customer != null){
            return "登录成功";
        }else {
            return "error";
        }

    }
}
