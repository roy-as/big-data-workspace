package com.as.sparkonhive

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object ThriftDemo {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn: Connection = DriverManager.getConnection("jdbc:hive2://node2:10000/default",
      "root",
      "123456")

    val ps: PreparedStatement = conn.prepareStatement("select * from student")

    val res: ResultSet = ps.executeQuery()

    while(res.next()) {
      val id: String = res.getString("id")
      val name: String = res.getString("name")
      val age: Int = res.getInt("age")
      println(s"id:$id,name:$name,age:$age")
    }
  }

}
