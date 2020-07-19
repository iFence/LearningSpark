package it.aspirin.sink

import java.sql._

import org.apache.spark.sql.{ForeachWriter, Row}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * 实现一个Spark的MySQL sink
 *
 * @param url
 * @param user
 * @param pwd
 */
class MysqlSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }


  def process(value: Row): Unit = {
    // 先查询数据库中是否已经有该条记录（因为按照user分组，user基本可以认为是key）
    val rs = statement.executeQuery(s"""select user from click_count where user = "${value.get(0)}" """)
    if (rs.next()) {
      val user = rs.getString(1)
      statement.executeUpdate(s""" update click_count set count = ${value.get(1)} where user = "$user" """)
    } else {
      statement.executeUpdate(s""" insert into click_count values ( "${value.get(0)}", "${value.get(1)}" ) """)
    }

    //    statement.executeUpdate(s"""INSERT INTO click_events VALUES ( "${value.get(0)}", "${value.get(1)}", "${value.get(2)}", "${value.get(3)}" )""")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}
