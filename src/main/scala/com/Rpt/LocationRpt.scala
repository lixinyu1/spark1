package com.Rpt

import java.sql.{Connection, Statement}

import com.utils.{ConnectionPool, JDBCUtil, RptUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

/**
  * 地域分布指标
  */
object LocationRpt {
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

    // 获取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    // 将数据进行处理，统计各个指标
    val province_city = df.map(row => {
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
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标
      ((pro, city), (RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin,
        adorderid, WinPrice, adpayment)))

    }).reduceByKey((x, y) => x.zip(y).map(x => x._1 + x._2))
      .map(x => (x._1._1, (x._1._2, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8))))
      .sortBy(x => x._1).cache()
    val province = province_city.reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8, x._9 + y._9, x._10 + y._10))
      .map(x => (x._1 + "1:--" + x._1, (x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10)))
    val city = province_city.map(x => (x._1 + ":  " + x._2._1, (x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10)))
    val finalRdd = (province union city)
      .sortByKey()
      .map(x => (x._1.split(":")(1), x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9))
      .map(x => (x._1, x._2, x._3, x._4, x._5, x._6, x._6 / (if (x._5 == 0) 1 else x._5) + "%", x._7, x._8, x._8 / (if (x._7 == 0) 1 else x._7) + "%", x._10, x._9))
      .foreachPartition(item => {
        val conn: Connection = ConnectionPool.getConnection
        val ps = conn.prepareStatement("insert into location values (?,?,?,?,?,?,?,?,?,?,?,?)")
        item.foreach(x => {
          JDBCUtil.insert(ps, x)
        })
        ConnectionPool.close(ps, conn)
      })
    sc.stop()
  }
}
