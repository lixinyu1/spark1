package com.utils

import java.io.{BufferedReader, FileReader}
import java.sql.{SQLException, _}
import java.util.Properties


object JDBCUtil {

  private val prop = new Properties()
  prop.load(new BufferedReader(new FileReader("G:\\ijworkspace\\GP_22_DMP\\src\\main\\resources\\source.properties")))
  Class.forName(prop.getProperty("driver"))

  def getConnection(): Connection = {
    val host = prop.getProperty("host")
    val user = prop.getProperty("user")
    val password = prop.getProperty("password")
    DriverManager.getConnection(host, user, password)

  }

  def close(resultSet: ResultSet, statement: Statement, connection: Connection): Unit = {
    if (resultSet != null) {
      try {
        resultSet.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    if (statement != null) {
      try {
        statement.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    if (connection != null) {
      try {
        connection.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  def close(statement: Statement, connection: Connection): Unit = {
    close(null, statement, connection)
  }

  def close(connection: Connection):Unit = {
    close(null, null, connection)
  }


  def insert(ps: PreparedStatement,x: (String, Double, Double, Double, Double, Double, String, Double, Double, String, Double, Double))={
    ps.setObject(1, x._1)
    ps.setObject(2, x._2)
    ps.setObject(3, x._3)
    ps.setObject(4, x._4)
    ps.setObject(5, x._5)
    ps.setObject(6, x._6)
    ps.setObject(7, x._7)
    ps.setObject(8, x._8)
    ps.setObject(9, x._9)
    ps.setObject(10, x._10)
    ps.setObject(11, x._11)
    ps.setObject(12, x._12)
    ps.executeUpdate()

  }
}
