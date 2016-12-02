package com.recsystem.tmp

import java.security.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.net.ntp.TimeStamp

/**
 * Created by lee on 2016/5/20.
 */
object Test2 {

  def main(args:Array[String]): Unit ={
    val sf:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val datetime = "2016-05-20 18:59:59"
    var rtn = ""
    if(datetime!=null && datetime.length>11){
      val timestr = datetime.substring(11, datetime.length)
      val d = sf.parse(timestr).getTime
      if(isInZone(d, "00:00:00", "05:59:59")){
        rtn = "凌晨"
      }else if(isInZone(d, "06:00:00","11:59:59")){
        rtn = "上午"
      }else if(isInZone(d, "12:00:00","17:59:59")){
        rtn = "下午"
      }else if(isInZone(d, "18:00:00","23:59:59")){
        rtn = "晚上"
      }

      System.out.println(rtn)
    }
  }

  def isInZone(time:Long, start:String, end:String): Boolean ={
    val sf:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val starttime = sf.parse(start).getTime
    val endtime = sf.parse(end).getTime
    return time>=starttime && time<=endtime
  }

}
