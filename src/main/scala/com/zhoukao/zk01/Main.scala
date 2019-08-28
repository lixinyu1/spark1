package com.zhoukao.zk01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jsonRdd: RDD[String] = sc.textFile("G:\\ijworkspace\\GP_22_DMP\\src\\main\\dir\\json.txt")

//  1ti
    //过滤掉空的没有商圈的
    jsonRdd.map(x => JSONGetPois.getBusinessArea(x)).filter(_._1!="[]").collect().toBuffer.foreach(println)
    println("------")
    //2ti
    //将所有数据合并并求每个type的总数
    jsonRdd.map(x => JSONGetPois.getTypes(x))
      .flatMap(x => x)
        .reduceByKey(_+_)
      .collect().toBuffer.foreach(println)
  }
}
