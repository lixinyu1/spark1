package com.zhoukao.zk01

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

object JSONGetPois {

  def getBusinessArea(string: String): (String, Int) = {
    //对JSON解析并过滤不包含目标数据的JSON
    val json = JSON.parseObject(string)
    if (json.getIntValue("status") == 0) return ("无",1)
    val regeocodeJSON = json.getJSONObject("regeocode")
    if (regeocodeJSON == null || regeocodeJSON.keySet().isEmpty) return ("无",1)
    val poisArray = regeocodeJSON.getJSONArray("pois")
    if (poisArray == null || poisArray.isEmpty) return ("无",1)
    val buffer = collection.mutable.ListBuffer[(String, Int)]()
    for (item <- poisArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        if (item.asInstanceOf[JSONObject].getString("businessarea").isEmpty||item.asInstanceOf[JSONObject].getString("businessarea")==null) {
          buffer.append(("无", 1))
        } else {
          //将每个地点计数为1
          buffer.append((item.asInstanceOf[JSONObject].getString("businessarea"), 1))
        }
      }
    }
    //求每个商圈的各自总数
    val stringToTuples: Map[String, ListBuffer[(String, Int)]] = buffer.groupBy(_._1)
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.foldLeft[Int](0)(_ + _._2))
    val array: Array[(String, Int)] = stringToInt.toArray
    (array(0)._1, array(0)._2)
  }

  def getTypes(string: String): Array[(String, Int)] = {
    //对JSON解析并过滤不包含目标数据的JSON
    val json = JSON.parseObject(string)
    if (json.getIntValue("status") == 0) return Array(("无",1))
    val regeocodeJSON = json.getJSONObject("regeocode")
    if (regeocodeJSON == null || regeocodeJSON.keySet().isEmpty) return Array(("无",1))
    val poisArray = regeocodeJSON.getJSONArray("pois")
    if (poisArray == null || poisArray.isEmpty) return Array(("无",1))
    val buffer = collection.mutable.ListBuffer[Array[(String, Int)]]()
    for (item <- poisArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        if ((item.asInstanceOf[JSONObject].getString("type").isEmpty)) {
          buffer.append(Array(("无", 1)))
        } else {
          val tuples: Array[(String, Int)] = item.asInstanceOf[JSONObject].getString("type").split(";")
            .map((_, 1))
          buffer.append(tuples)
        }
      }
    }
    //求该条JSON对应的type的数量
    val stringToTuples: Map[String, ListBuffer[(String, Int)]] = buffer.flatten
      .groupBy(_._1)
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.foldLeft(0)(_+_._2))
    val array: Array[(String, Int)] = stringToInt.toArray
    array
  }

}
