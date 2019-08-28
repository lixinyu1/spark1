package com.utils

import akka.io.Udp.SO.Broadcast

import scala.collection.mutable

/**
  * 指标方法
  */
object RptUtils {

  // 此方法处理请求数
  def request(requestmode: Int, processnode: Int): List[Double] = {
    if (requestmode == 1) {
      processnode match {
        case 1 => List(1, 0, 0)
        case 2 => List(1, 1, 0)
        case 3 => List(1, 1, 1)
      }
    } else {
      List(0, 0, 0)
    }
  }

  // 此方法处理展示点击数

  def click(requestmode: Int, iseffective: Int): List[Double] = {
    iseffective match {
      case 1 => requestmode match {
        case 2 => List(1, 0)
        case 3 => List(0, 1)
      }
      case _ => List(0, 0)
    }

  }

  // 此方法处理竞价操作

  def Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, WinPrice: Double, adpayment: Double): List[Double] = {

    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      if (iswin == 1 && adorderid != 0) {
        List[Double](1, 1, WinPrice / 1000, adpayment / 1000)
      } else {
        List[Double](1, 0, 0, 0)
      }
    } else {
      List[Double](0, 0, 0, 0)
    }

  }

  def GetAppName(appId: String, appName: String, map_dict: mutable.Map[String, String]): String = {
    if (appName.length < 1 || (appName.length > 0 && appName == appId)) {
      map_dict.getOrElse(appId, "未知")
    } else {
      appName
    }
  }

}