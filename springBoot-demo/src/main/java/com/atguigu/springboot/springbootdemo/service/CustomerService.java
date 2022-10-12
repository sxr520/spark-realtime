package com.atguigu.springboot.springbootdemo.service;

import org.springframework.stereotype.Service;

/**
*
 */

@Service //表示成业务层
public interface CustomerService {
String doLogin(String username,String password);
}
