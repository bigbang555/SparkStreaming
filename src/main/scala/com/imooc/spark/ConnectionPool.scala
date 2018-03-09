package com.imooc.spark

import com.mysql.jdbc.Connection

object ConnectionPool {
  def getConnection() = {

  }

  def returnConnection(connection: Connection) = {
    connection.close()
  }
}
