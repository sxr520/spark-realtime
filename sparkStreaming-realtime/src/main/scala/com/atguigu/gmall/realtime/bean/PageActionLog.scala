package com.atguigu.gmall.realtime.bean

/**
 * @author sxr
 * @create 2022-06-02-22:45
 */
case class PageActionLog(
                          mid :String,
                          user_id:String,
                          province_id:String,
                          channel:String,
                          is_new:String,
                          model:String,
                          operate_system:String,
                          version_code:String,
                          brand:String,
                          page_id:String ,
                          last_page_id:String,
                          page_item:String,
                          page_item_type:String,
                          during_time:Long,
                          sourceType:String,
                          action_id:String,
                          action_item:String,
                          action_item_type:String,
                          ts:Long
                        ){
}
