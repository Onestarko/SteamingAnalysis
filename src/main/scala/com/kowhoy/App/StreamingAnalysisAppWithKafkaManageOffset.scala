package com.kowhoy.App

import java.util.Date

import com.alibaba.fastjson.JSON
import com.kowhoy.Utils.CommonUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @DESC streaming流处理分析kafka订单数据,将结果保存到redis,并使用kafka自身进行offset管理
 * @Date 2020/7/8 10:26 上午
 **/

object StreamingAnalysisAppWithKafkaManageOffset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingAnalysisAppWithKafkaManageOffset")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10)) // 10s每批

    ssc.sparkContext.setLogLevel("WARN")

    // 设置kafka消费参数
    val kafkaParams = scala.collection.mutable.Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getConfigFiled("KAFKA_BROKER_LIST"),
      "key.deserializer" -> classOf[StringDeserializer], // key的反序列化
      "value.deserializer" -> classOf[StringDeserializer], // value的反序列化
      "group.id" -> "streaming_kafka_none",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = CommonUtil.getConfigFiled("KAFKA_TOPIC")

    val topics = List(topic)

    val streamRDD = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val timeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    streamRDD.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val data = rdd.map(x => JSON.parseObject(x.value))
        .map(log => {
          val flag = log.getString("flag")
          val fee = log.getLong("fee")
          val time = log.getString("time")

          val day = time.substring(0, 10)
          val hour = time.substring(11, 13)
          val minute = time.substring(14, 16)

          val success:(Long, Long) = if (flag == "1") (1, fee) else (0, 0)

          (day, hour, minute, List[Long](1, success._1, success._2))
        })

      data.map(x => (x._1, x._4)).reduceByKey((a, b) => {
        a.zip(b).map(x => x._1 + x._2)
      }).foreachPartition(part => {
        val jedis = CommonUtil.getRedis()
        part.foreach(x => {
          jedis.hincrBy("n-ko-"+x._1, "total", x._2(0))
          jedis.hincrBy("n-ko-"+x._1, "success", x._2(1))
          jedis.hincrBy("n-ko-"+x._1, "fee", x._2(2))
        })
        println("saved at " + timeFormat.format(new Date()))
      })

      streamRDD.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
