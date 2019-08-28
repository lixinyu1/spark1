package com.utils

import com.utils.JedisUtils.jedisPool
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * 测试类
  */
object test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val jedisPool = new JedisPool("192.168.81.101")
//    val conn: Jedis = jedisPool.getResource
    val list= List("116.310003,39.991957")
    val rdd = sc.makeRDD(list)
    val bs = rdd.map(t=> {
      val arr = t.split(",")
      AmapUtil.getBusinessFromAmap(arr(0).toDouble,arr(1).toDouble)
    })
    bs.foreach(println)
  }
}
