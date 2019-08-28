package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsLocation extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型，广告类型名称
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    list:+=("ZP"+provincename,1)
    list:+=("ZC"+cityname,1)
    list
  }
}
