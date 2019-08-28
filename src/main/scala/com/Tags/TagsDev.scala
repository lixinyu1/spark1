package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsDev extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")
    val networkmannername = row.getAs[String]("networkmannername")
    val ispname = row.getAs[String]("ispname")

    client match {
      case 1 => list :+= ("1 Android D00010001", 1)
      case 2 => list :+= ("2 IOS D00010002", 1)
      case 3 => list :+= ("3 WinPhone D00010003", 1)
      case _ => list :+= ("_ 其 他 D00010004", 1)
    }

    networkmannername match {
      case "Wifi" => list :+= ("WIFI D00020001", 1)
      case "4G" => list :+= ("4G D00020002", 1)
      case "3G" => list :+= ("3G D00020003", 1)
      case "2G" => list :+= ("2G D00020004", 1)
      case _ => list :+= ("_   D00020005", 1)
    }

    ispname match {
      case "移动" => list :+= ("移 动 D00030001", 1)
      case "联通" => list :+= ("联 通 D00030002", 1)
      case "电信" => list :+= ("电 信 D00030003", 1)
      case _ => list :+= ("_ D00030004", 1)
    }
    list
  }
}
