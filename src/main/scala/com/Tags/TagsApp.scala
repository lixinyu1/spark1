package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable

object TagsApp extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val dict_map = args(1).asInstanceOf[collection.Map[String, String]]
    //    val conn=args(1).asInstanceOf[Jedis]
    //    val key=args(2).asInstanceOf[String]
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")

    //map直接获取字典
    if (!StringUtils.isNotBlank(appname) || (StringUtils.isNotBlank(appname) && appname == appid)) {
      val appname = dict_map.getOrElse(appid, "未知")
    }

    //redis中获取字典
    //    if (!StringUtils.isNotBlank(appname) || (StringUtils.isNotBlank(appname) && appname == appid)) {
    //      conn.hset("in",appid,appname)
    //      appname = conn.hget(key, appid)
    //      conn.hset("in",appid,appname)
    //    }

    list :+= ("APP" + appname, 1)
    list
  }
}
