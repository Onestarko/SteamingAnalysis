package com.kowhoy.Utils

import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * @DESC 配置项相关工具类
 * @Date 2020/7/3 2:14 下午
 **/

object CommonUtil {
  /***
   * @Desc 根据键值获取配置项
   * @Date 9:36 上午 2020/7/3
   * @Param [key]
   * @Return java.lang.String
   **/
  def getConfigFiled(key: String): String = {
    val conf = ConfigFactory.load()

    var value: String = null;

    try {
      value = conf.getString(key)
    }catch {
      case _:Exception => System.err.println(key + " not exists")
    }

    value
  }

  /***
    * @Desc 获取REDIS连接
    * @Date 11:23 上午 2020/7/7
    * @Param []
    * @Return redis.clients.jedis.Jedis
  **/
  def getRedis(): Jedis = {
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxIdle(10)
    poolConfig.setMaxTotal(1000)

    lazy val jedisPool = new JedisPool(poolConfig, CommonUtil.getConfigFiled("REDIS_HOST"))

    val jedis = jedisPool.getResource
    jedis.select(CommonUtil.getConfigFiled("REDIS_DB").toInt)
    jedis
  }
}
