package com.atguigu.springboot.springbootdemo.bean;

import lombok.*;

/**
 * @author sxr
 * @create 2022-07-28-16:46
 */


@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Customer {
    public String username;
    public String pwd;
    public String address;
    public Integer age;
    public String gender;

}
