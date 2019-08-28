import com.utils.ConnectionPool
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val map: mutable.Map[String, String] = mutable.Map()
    val arr1 = Array(List(("1", 1), ("2", 2), ("3", 3)), List(("1", 1), ("2", 2), ("3", 3)))
    val rdd1: RDD[List[(String, Int)]] = sc.makeRDD(arr1)
    val value: RDD[(String, Int)] = rdd1.flatMap(x => x)
    val flatten: Array[(String, Int)] = arr1.flatten
    arr1.flatMap(x => {
//      val x: List[(String, Int)] = x
      x
    })
    val arr2 = Array(("1", 1), ("2", 2), ("3", 3), ("1", 1), ("2", 2), ("3", 3))
    //    val flatten2: Array[Nothing] = arr2.flatten
    flatten.toBuffer.foreach(println)
    val arr21 = sc.parallelize(Array(List(("1", 1), ("2", 2), ("3", 3)), List(("1", 1), ("2", 2), ("3", 3))))
    arr21.flatMap(x => x).foreach(println)

    val list3 = List("hadoop", "spark", "hive", "spark")
    val rdd = sc.parallelize(list3)
    val pairRdd = list3.map(x => (x, 1))
    val stringToTuples: Map[String, List[(String, Int)]] = pairRdd.groupBy(_._1)
    stringToTuples
      .toBuffer.foreach(println)
    var list = List[(String, Int)]()
    val iterator: Iterator[(String, Int)] = list.iterator
    val strings = "a,b,c,d".split(",")
    for (a <- 0 to strings.length - 1) {
      println(strings(a))
    }

    println(map + ("a" -> "1"))
    println(map)
    map.update("b", "2")
    println(map)
    println(map.contains("b"))
    println("".isEmpty)
    //    val connection = ConnectionPool.getConnection
    //    val statement = connection.createStatement()
    //    val a="111"
    //    statement.executeUpdate(s"insert into location values  ('${a}',${a},${a},${a},${a},${a},${a},${a},${a},${a},${a},${a})")
    val a =
    """
      |1
      |2
      |3
    """.stripMargin
    println(a)
    println(StringUtils.isNoneBlank(""))
    println(StringUtils.isNoneBlank(" "))
    println(StringUtils.isNotBlank(""))
    println(StringUtils.isNotBlank(" "))
    println(StringUtils.isNoneEmpty(""))
    println(StringUtils.isNoneEmpty(" "))
    println(StringUtils.isNotEmpty(""))
    println(StringUtils.isNotEmpty(" "))
    println("1|2|3".split("\\|")(0))
    println(Array(("aa", 1)).contains("aa"))
  }

  println("aaaa")
}
