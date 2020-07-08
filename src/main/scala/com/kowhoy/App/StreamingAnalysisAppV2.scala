package com.kowhoy.App

import com.alibaba.fastjson.JSON
import com.kowhoy.Utils.CommonUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition

/**
 * @DESC
 * @Date 2020/7/7 1:36 下午
 **/

object StreamingAnalysisAppV2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingAnalysisApp").setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")

    val topics = CommonUtil.getConfigFiled("KAFKA_TOPIC").split(",")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getConfigFiled("KAFKA_BROKER_LIST"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> CommonUtil.getConfigFiled("KAFKA_GROUP_ID"),
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topicDirs = new ZKGroupTopicDirs(CommonUtil.getConfigFiled("KAFKA_GROUP_ID"), topics(0))

    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val zkClient = new ZkClient(CommonUtil.getConfigFiled("ZK_HOST"))

    val children = zkClient.countChildren(zkTopicPath)

    val streamRDD = if (children > 0) {
     // 已经消费过
      var fromOffsets = Map[TopicPartition, Long]()

      (0 until children).foreach(partitionId => {
        val offset = zkClient.readData[String](zkTopicPath+s"/${partitionId}")

        fromOffsets += (new TopicPartition(topics(0), partitionId) -> offset.toLong)
      })

       KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )
    } else {
      // 第一次启动
      KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    }

    streamRDD.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
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
          print("f_day", x)
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
          println("f_hour", x)
        })
        println("add_hour")
      })

      for (o <- offsetRange) {
        ZkUtils(zkClient, false).updatePersistentPath(zkTopicPath+"/"+o.partition, o.untilOffset.toString)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
