package com.kowhoy.Utils

import java.util
import java.util.{Date, Properties, Random, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @DESC Kafka生產數據
 * @Date 2020/7/3 2:33 下午
 **/

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    val props = new Properties()

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", CommonUtil.getConfigFiled("KAFKA_BROKER_LIST"))
    props.put("request.required.acks", "1")

//    request.required.acks參數說明:
//    0 表示 producer不等待broker同步完成確認之後發送下一條數據
//    1 表示 producer在leader已成功收到數據完成確認之後發送下一條數據
//    -1 表示 producer在follower副本中確認收到數據後之後發送下一條數據
//
//    0 -> 1 -> -1 性能降低、數據健壯性增強

    val producer = new KafkaProducer[String, String](props)

    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val topic = CommonUtil.getConfigFiled("KAFKA_TOPIC")

    for (i <- 0 to 9) {
      val time = dateFormat.format(new Date())
      val userId = random.nextInt(1000).toString
      val courseId = random.nextInt(500).toString
      val fee = random.nextInt(500).toString
      val result = Array("0", "1")
      val flag = result(random.nextInt(2))
      val orderId = UUID.randomUUID().toString

      val map = new util.HashMap[String, Object]()
      map.put("time", time)
      map.put("userId", userId)
      map.put("courseId", courseId)
      map.put("fee", fee)
      map.put("flag", flag)
      map.put("orderId", orderId)

      val json = new JSONObject(map)

      producer.send(new ProducerRecord[String, String](topic, json.toString()))
    }

    println("kafka生產完成")

    producer.close()
  }
}
