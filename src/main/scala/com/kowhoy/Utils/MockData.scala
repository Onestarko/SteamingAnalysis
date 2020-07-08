package com.kowhoy.Utils

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat

import scala.util.Random

/**
 * @DESC mock数据
 * @Date 2020/7/3 2:17 下午
 **/

object MockData {
  def main(args: Array[String]): Unit = {
    val random = new Random()

    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    for (i <- 0 to 9) {
      val time = dateFormat.format(new Date()).toString
      val userId = random.nextInt(1000).toString
      val courseId = random.nextInt(500).toString
      val fee = random.nextInt(500).toString
      val result = Array("0", "1")
      val flag = result(random.nextInt(2)).toString
      val orderId = UUID.randomUUID().toString

      val map = new util.HashMap[String, Object]()

      map.put("time", time)
      map.put("userId", userId)
      map.put("courseId", courseId)
      map.put("fee", fee)
      map.put("flag", flag)
      map.put("orderId", orderId)

      val json = new JSONObject(map)

      println(json)
    }


  }
}
