package com.Rpt

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.collection.mutable

object MediaRpt {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //    val dic_map:[String,String]=Map()
    //    val broad=sc.broadcast(dic_map)
    val array_dict:Array[(String,String)] = sc.textFile("G:\\ijworkspace\\GP_22_DMP\\src\\main\\dir\\app_dict.txt")
      .map(x => x.split("\t"))
      .filter(_.length > 0)
      .map(x => (x(4), x(1)))
      .collect()
    val map_dict:mutable.Map[String,String]=mutable.Map()
    array_dict.map(x=>map_dict+=(x._1->x._2))
    val broad=sc.broadcast(map_dict)

    // 获取数据

    val df: DataFrame = sQLContext.read.parquet(inputPath)

    // 将数据进行处理，统计各个指标
    val rdd = df.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  媒体id  媒体名称
      val appId = row.getAs[String]("appid")
      val appName = row.getAs[String]("appname")
      val map_dict: mutable.Map[String, String] = broad.value
      // 创建三个对应的方法处理九个指标
      (RptUtils.GetAppName(appId,appName,map_dict), (RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin,
        adorderid, WinPrice, adpayment)))

    })
      .reduceByKey((x, y) => x.zip(y).map(x => x._1 + x._2))
      .map(x => (x._1, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
      .sortBy(_._1)

    rdd.collect().toBuffer.foreach(println)

    sc.stop()
  }
}
