package com.Tags

import java.sql.Connection

import com.typesafe.config.ConfigFactory
import com.utils.{ConnectionPool, JDBCUtil, JedisUtils, TagUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //    if(args.length != 5){
    //      println("目录不匹配，退出程序")
    //      sys.exit()
    //
    //    }
    val Array(inputPath, dirPath, stopword, days) = args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val frame = sQLContext.read.parquet(inputPath)

    // todo 调用Hbase API
    // 加载配置文件
//    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.TableName")
//    // 创建Hadoop任务
//    val configuration = sc.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))
//    // 创建HbaseConnection
//    val hbconn = ConnectionFactory.createConnection(configuration)
//    val hbadmin = hbconn.getAdmin
//    // 判断表是否可用
//    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
//      // 创建表操作
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      val descriptor = new HColumnDescriptor("tags")
//      tableDescriptor.addFamily(descriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbconn.close()
//    }
//    // 创建JobConf
//    val jobconf = new JobConf(configuration)
//    // 指定输出类型和表
//    jobconf.setOutputFormat(classOf[TableOutputFormat])
//    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    //过滤字典
    val stopwords = sc.textFile(stopword)
      .collect()
    val broadstopword = sc.broadcast(stopwords)

//    appName  Redis
        val dict_map = sc.textFile(dirPath)
          .map(x => x.split("\t"))
          .filter(x => x.length >= 5)
          .map(x => (x(4), x(1)))
          .collectAsMap
    val broad_dict = sc.broadcast(dict_map)
    //    val connection: Jedis = JedisUtils.getConnection
    val key = "dict"
    //    dict_map.foreach(x => connection.hset(key, x._1, x._2))
    //    connection.close()
    // 读取数据
    val df = frame
    // 过滤符合Id的数据
    //广告位
    //    df.filter(TagUtils.OneUserId)
    //      // 接下来所有的标签都在内部实现
    //        .map(row=>{
    //          // 取出用户Id
    //          val userId = TagUtils.getOneUserId(row)
    //         // 接下来通过row数据 打上 所有标签（按照需求）
    //        val adList = TagsAd.makeTags(row)
    //      (userId,adList)
    //        }).collect().toBuffer.foreach(println)

    //App 广播
    //    df.filter(TagUtils.OneUserId)
    //      .map(row => {
    //        val userId = TagUtils.getOneUserId(row)
    //        val adList = TagsApp.makeTags(row, broad.value)
    //        (userId, adList)
    //      }
    //      ).collect().toBuffer.foreach(println)

    //App Redis  foreachPartition
    //    df.filter(TagUtils.OneUserId)
    //      .foreachPartition(item => {
    //        val conn = JedisUtils.getConnection
    //        item.foreach(row => {
    //          val userId = TagUtils.getOneUserId(row)
    //          val adList = TagsApp.makeTags(row, conn, key)
    //          (userId, adList)
    //        })
    //        conn.close()
    //      }
    //      )


    df.filter(TagUtils.OneUserId)
      .mapPartitions(item => {
//        val conn = JedisUtils.getConnection
        var list = List[(String, List[(String, Int)])]()
        val tuples: Iterator[(String, List[(String, Int)])] = item.map(row => {
          val userId = TagUtils.getOneUserId(row)
          val adList = TagsAd.makeTags(row)
          val appList = TagsApp.makeTags(row, broad_dict.value)
          val platList = TagsPlat.makeTags(row)
          val devList = TagsDev.makeTags(row)
          val keysList = TagsKey.makeTags(row, broadstopword.value)
          val locationList = TagsLocation.makeTags(row)
//          val business = BusinessTag.makeTags(row)
          (userId, adList ++ appList ++ platList ++ devList ++ devList ++ keysList ++ locationList )
        })
//        conn.close()
        tuples
      })
      .reduceByKey((x, y) => {
        val stringToTuples: Map[String, List[(String, Int)]] = (x ::: y)
          .groupBy(_._1)
        stringToTuples
          .mapValues(_.foldLeft[Int](0)(_ + _._2))
          .toList
      }
      )
      .take(20).toBuffer.foreach(println)
//      .map {
//      case (userid, userTag) => {
//
//        val put = new Put(Bytes.toBytes(userid))
//        // 处理下标签
//        val tags = userTag.map(t => t._1 + "," + t._2).mkString(",")
//        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$days"), Bytes.toBytes(tags))
//        (new ImmutableBytesWritable(), put)
//      }
//    }
//      .saveAsHadoopDataset(jobconf)


    //渠道
    //      df.filter(TagUtils.OneUserId)
    //        .map(row=>{
    //            val userId=TagUtils.getOneUserId(row)
    //            val adList=TagsPlat.makeTags(row)
    //            (userId,adList)
    //        }
    //        ).collect().toBuffer.foreach(println)

    //设备
    //    df.filter(TagUtils.OneUserId)
    //      .map(row=>{
    //        val userId=TagUtils.getOneUserId(row)
    //        val adList=TagsDev.makeTags(row)
    //        (userId,adList)
    //      }
    //      ).collect().toBuffer.foreach(println)
    //  }

    //关键字
    //    df.filter(TagUtils.OneUserId)
    //      .map(row => {
    //        val userId = TagUtils.getOneUserId(row)
    //        val adList = TagsKey.makeTags(row,broad.value)
    //        (userId, adList)
    //      }
    //      ).collect().toBuffer.foreach(println)

    //地域
    //    df.filter(TagUtils.OneUserId)
    //      .map(row => {
    //        val userId = TagUtils.getOneUserId(row)
    //        val adList = TagsLocation.makeTags(row)
    //        (userId, adList)
    //      }
    //      ).collect().toBuffer.foreach(println)
  }
}