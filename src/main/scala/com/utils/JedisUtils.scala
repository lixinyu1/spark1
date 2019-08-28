package com.utils

import java.util

import redis.clients.jedis.{Jedis, JedisPool}

object JedisUtils {
  val jedisPool = new JedisPool("192.168.81.101")

  def getConnection: Jedis = {
    val conn: Jedis = jedisPool.getResource
    conn
  }


}
