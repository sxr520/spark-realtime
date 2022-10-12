package com.atguigu.springboot.springbootdemo.controller;

import com.atguigu.springboot.springbootdemo.bean.Customer;

import com.atguigu.springboot.springbootdemo.service.CustomerService;
import com.atguigu.springboot.springbootdemo.service.impl.CustomerServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import javax.lang.model.element.VariableElement;


/**
 * @author sxr
 * @create 2022-07-28-14:44
 */



class MyFactoryUtils{

    public static CustomerService newInstance(){
        return new CustomerServiceImpl();
//        return new CustomerServiceImplNew();
    }
}


//控制层
//@Controller    //标识为控制层(spring)
@RestController  // @Controller  + @ResponseBody
public class customerController {

    //直接在类中创建对象，不好， 写死了.
    //CustomerServiceImpl customerService = new CustomerServiceImpl();

//    CustomerService customerService = new CustomerServiceImpl();

//    CustomerService customerService = MyFactoryUtils.newInstance();

    @Autowired //从spring容器中找到对应类型的容器的对象，注入过来
    @Qualifier("customerServiceImpl") //明确指定哪个对象注入过来
    CustomerService customerService;

    /**
     * http://localhost:8080/login?username=sxr&password=123456
     */
    @GetMapping("login")
    public String login(@RequestParam("username") String username ,
                        @RequestParam("password") String password){
        //业务处理
        //在每个方法中创建业务层对象， 不好.
//        CustomerService customerService = new CustomerService();
        String result = customerService.doLogin(username, password);
        return result;
    }






















    /**
     *请求处理方法
     *
     * @RequestMapping 将客户端的请求和方法进行映射
     *
     * @ResponseBody: 将方法的返回值处理成字符串(json)返回给客户端
     */
    @RequestMapping("hello")
//    @ResponseBody
    public String hello(){
        return "success";
    }

    @RequestMapping("param")
    public String paramkv(@RequestParam("username") String name, Integer age){
        return "name= "+name+"; age= "+age;
    }

    @RequestMapping("path/{username}/{age}")
    public String path(@PathVariable("username") String username,
                       @PathVariable("age") Integer age,
                       @RequestParam("address") String address
    ){
        return "username= "+username+"; age= "+age+", address="+address;
    }

    @RequestMapping("parambody")
    public  String parambody(String username,String pwd){
        if(username.equals("sxr")&& pwd.equals("123456")){
            return "登录成功";
        }else {
            return "登录失败";
        }
    }


    /**
     * 如果请求参数名与方法的形参名不一致，需要通过@RequestParam来标识获取
     * 如果一致，可以直接映射.
     *
     * @RequestBody ：将请求体中的参数映射到对象中对应的属性上.
     */
    @RequestMapping("parambody2")
    public Customer parambody2(@RequestBody Customer customer){
        return customer; //转换为json
    }

//    @RequestMapping(value = "requestmethod",method = RequestMethod.GET)
    @GetMapping("requestmethod")
    public String requsetmethod(){
        return "sucess";
    }

    /**
     * 请求方式 :  GET  POST  PUT  DELETE ....
     *  GET  :  读
     *  POST :  写
     */

    /**
     * 常见的状态码:
     *
     *  200 : 表示请求处理成功且响应成功
     *  302 : 表示进行重定向
     *  400 : 表示请求参数有误
     *  404 : 表示请求地址或者资源不存在
     *  405 : 表示请求方式不支持
     *  500 : 表示服务器端处理异常.
     *
     * http://localhost:8080/statuscode
     */

    @GetMapping("statuscode")
    public String statusCode(@RequestParam("username") String username , @RequestParam("age") Integer age ){
        return username + " , " + age ;
    }

}
