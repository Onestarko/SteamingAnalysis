package com.kowhoy.App

import java.util.Date

import com.alibaba.fastjson.JSON
import com.kowhoy.Utils.CommonUtil
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

/**
 * @DESC streaming流处理分析kafka订单数据,将分析结果保存到redis中,并使用zookeeper管理offset
 * @Date 2020/7/8 10:24 上午
 **/

object StreamingAnalysisAppWithZookeeperManageOffset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingAnalysisAppWithZookeeperManageOffset")
      .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")

    val groupId = "zookeeper-manage-group"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getConfigFiled("KAFKA_BROKER_LIST"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = CommonUtil.getConfigFiled("KAFKA_TOPIC")
    val topics = List(topic)

    val topicDirs = new ZKGroupTopicDirs(groupId, topic)

    val zkTopicPath = topicDirs.consumerOffsetDir

    val zkClient = new ZkClient(CommonUtil.getConfigFiled("ZK_HOST"))

    val children = zkClient.countChildren(zkTopicPath)

    val streamRDD = if (children > 0) {
      println("已经消费过...")

      var fromOffsets = Map[TopicPartition, Long]()

      for (partitionId <- 0 until children) {
        val offset = zkClient.readData[String](zkTopicPath + "/" + partitionId)

        fromOffsets += (new TopicPartition(topic, partitionId) -> offset.toLong)
      }

      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )
    } else {
      println("第一次进行消费...")

      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }

    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

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
      }).foreachPartition(
        part => {
          val jedis = CommonUtil.getRedis()
          part.foreach(x => {
            jedis.hincrBy("zk-ko-"+x._1, "total", x._2(0))
            jedis.hincrBy("zk-ko-"+x._1, "success", x._2(1))
            jedis.hincrBy("zk-ko-"+x._1, "fee", x._2(2))
          })

          println("saved at " + dateFormat.format(new Date()))
        }
      )

      for (o <- offsetRange) {
        ZkUtils(zkClient, false).updatePersistentPath(zkTopicPath +"/"+o.partition, o.untilOffset.toString)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
