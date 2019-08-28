package com.utils

import java.sql.{Connection, DriverManager, Statement}
import java.util._

import scala.collection.mutable

object ConnectionPool {
  private val capicity = 50
  private val multiplex = 20
  private var countConnections = 0
  private val list: LinkedList[Connection] = new LinkedList()


  for (i <- 1 to 10) {
    list.add(JDBCUtil.getConnection())
  }

  def getConnection: Connection = {
    "".synchronized {
      if (list.size() > 0) {
        list.removeFirst()
      } else {
        if (countConnections >= capicity) {
          while (countConnections >= capicity) {
          }
          JDBCUtil.getConnection()
        } else {
          JDBCUtil.getConnection()
        }
      }
    }
  }

  def close(connection: Connection): Unit = {
    "".synchronized {
      if (list.size() >= multiplex) {
        JDBCUtil.close(connection)
      } else {
        list.add(connection)
      }
    }
  }
  def close(statement:Statement,connection: Connection): Unit = {
    "".synchronized {
      if (list.size() >= multiplex) {
        JDBCUtil.close(statement,connection)
      } else {
        list.add(connection)
      }
    }
  }
}
