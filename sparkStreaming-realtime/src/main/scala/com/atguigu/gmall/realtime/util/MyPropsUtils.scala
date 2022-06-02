package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

/**
 * @author sxr
 * @create 2022-05-29-23:10
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey:String): String ={
    bundle.getString(propsKey)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils.apply("kafka_bootstrap_servers"))
    println(MyPropsUtils("kafka_bootstrap_servers"))
  }

}
