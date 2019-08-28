package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsKey extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Array[String]]
    val keywords = row.getAs[String]("keywords")

    keywords.split("\\|")
      .filter(x => x.length > 2 && x.length < 9 && !stopword.contains(x))
      .foreach(x => list :+= ("K"+x, 1))

    list
  }
}
