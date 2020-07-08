package com.kowhoy.App

import com.alibaba.fastjson.JSON
import com.kowhoy.Utils.CommonUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @DESC 流失分析订单日志数据_v1
 * @Date 2020/7/7 10:06 上午
 **/

object StreamingAnalysisAppV1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    ssc.sparkContext.setLogLevel("WARN")

    val topics = CommonUtil.getConfigFiled("KAFKA_TOPIC").split(",")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getConfigFiled("KAFKA_BROKER_LIST"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> CommonUtil.getConfigFiled("KAFKA_GROUP_ID"),
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val streamRDD = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    streamRDD.foreachRDD(rdd => {
      val data = rdd.map(x => JSON.parseObject(x.value)).map{
        log => {
          val flag = log.getString("flag")
          val fee = log.getLong("fee")
          val time = log.getString("time")

          val day = time.substring(0, 10)
          val hour = time.substring(11, 13)
          val minute = time.substring(14, 16)

          val success:(Long, Long) = if (flag == "1") (1, fee) else (0, 0)

          (day, hour, minute, List[Long](1, success._1, success._2))
        }
      }

      data.map(x => (x._1, x._4)).reduceByKey((a, b) => {
        a.zip(b).map(x => x._1 + x._2)
      }).foreachPartition(part => {
        val jedis = CommonUtil.getRedis()
        part.foreach(x => {
          jedis.hincrBy("order-"+x._1, "total", x._2(0))
          jedis.hincrBy("order-"+x._1, "success", x._2(1))
          jedis.hincrBy("order-"+x._1, "fee", x._2(2))
        })
        println("add_day")
      })

      data.map(x => ((x._1, x._2), x._4))
        .reduceByKey((a, b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(part => {
        val jedis = CommonUtil.getRedis()
        part.foreach(x => {
          jedis.hincrBy("order-"+x._1._1, "total"+x._1._2, x._2(0))
          jedis.hincrBy("order-"+x._1._1, "success"+x._1._2, x._2(1))
          jedis.hincrBy("order-"+x._1._1, "fee"+x._1._2, x._2(2))
        })
        println("add_hour")
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
