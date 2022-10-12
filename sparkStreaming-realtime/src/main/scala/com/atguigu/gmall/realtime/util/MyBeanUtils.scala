package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * @author sxr
 * @create 2022-07-16-19:11
 */
object MyBeanUtils {
  /**
   * 将srcObj中的属性拷贝到destObj对应的属性上。反射知识
   */
//  def main(args: Array[String]): Unit = {
//    val pageLog: PageLog = PageLog("mid123", "uid123", "p123", null, null, null, null, null, null, null, null, null, null, 0L, null, 1233454)
//    val dauInfo: DauInfo = new DauInfo()
//    println("拷贝前dauInfo："+dauInfo.toString)
//    copyProperties(pageLog,dauInfo)
//    println("拷贝后dauInfo："+dauInfo.toString)
//  }
  def copyProperties(srcObj:AnyRef,destObj:AnyRef): Unit ={
      if(srcObj == null || destObj == null){
        return
      }
    val fields: Array[Field] = srcObj.getClass.getDeclaredFields
    for (field <- fields) {
    Breaks.breakable{
      //get / set
      // Scala 会自动为类中的属性提供 get、 set 方法
      // get : fieldname()
      // set : fieldname_$eq(参数类型)
      val getMethodName : String = field.getName
      val setMethodName : String = field.getName + "_$eq"
      //从 srcObj 中获取 get 方法对象
      val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
      //从 destObj 中获取 set 方法对象
      val setMethod: Method = {
        try{
          destObj.getClass.getDeclaredMethod(setMethodName, field.getType)
        }catch {
          // NoSuchMethodException
          case ex:Exception => Breaks.break()
        }
      }
      //忽略 val 属性
      val destField: Field =
        destObj.getClass.getDeclaredField(field.getName)
      if(destField.getModifiers.equals(Modifier.FINAL)){
        Breaks.break()
      }
      //调用 get 方法获取到 srcObj 属性的值， 再调用 set 方法将获取到的属性值赋值给 destObj 的属性
      setMethod.invoke(destObj,getMethod.invoke(srcObj))
    }
    }

  }
}
