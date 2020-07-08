package com.kowhoy.App

import java.util.Date

import com.alibaba.fastjson.JSON
import com.kowhoy.Utils.CommonUtil
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang.time.FastDateFormat
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

/**
 * @DESC 流式分析 使用ZK进行offset的控制
 * @Date 2020/7/7 2:25 下午
 **/

object StreamingAnalysisAppWithOffset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingAnalysisApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getConfigFiled("KAFKA_BROKER_LIST"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> CommonUtil.getConfigFiled("KAFKA_GROUP_ID"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = CommonUtil.getConfigFiled("KAFKA_TOPIC")

    val topics = List(topic)

    val topicDirs = new ZKGroupTopicDirs(CommonUtil.getConfigFiled("KAFKA_GROUP_ID"), topic)

    val zkTopicPath = topicDirs.consumerOffsetDir // topic路径 /consumers/{group}/offsets/{topic}/...{partitions}

    val zkClient = new ZkClient(CommonUtil.getConfigFiled("ZK_HOST")) // zk连接

    val children = zkClient.countChildren(zkTopicPath) // partitions num


    val streamRDD = KafkaUtils.createDirectStream(
              ssc,
              LocationStrategies.PreferConsistent,
              ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
            )
//    val streamRDD = if (children > 0) {
//      println("已经消费过...")
//      var fromOffsets = Map[TopicPartition, Long]()
//
//      for (partitionId <- 0 until children) {
//         val offset = zkClient.readData[String](zkTopicPath + "/" + partitionId)
//
//        fromOffsets += (new TopicPartition(topic, partitionId) -> offset.toLong)
//      }
//
//      KafkaUtils.createDirectStream(
//        ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
//      )
//    } else {
//      println("第一次消费...")
//      KafkaUtils.createDirectStream(
//        ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//      )
//    }

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
          jedis.hincrBy("ko-"+x._1, "total", x._2(0))
          jedis.hincrBy("ko-"+x._1, "success", x._2(1))
          jedis.hincrBy("ko-"+x._1, "fee", x._2(2))
        })
        println("save at " + FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(new Date()))
      })

      streamRDD.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)

//      for (o <- offsetRange) {
//        ZkUtils(zkClient, false).updatePersistentPath(zkTopicPath+"/"+o.partition, o.untilOffset.toString)
//      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
