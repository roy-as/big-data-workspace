package com.as.structuredstreaming

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

object SocketDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val df = spark.readStream.format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()
    import spark.implicits._
    df.printSchema()
    val ds = df.as[String]


    val result = ds.flatMap(_.split("\\s+"))
      .groupBy('value)
      .count()
    //.orderBy('count.desc)

    //    result.writeStream
    //      .format("console")
    //      .outputMode("complete")
    //      .option("truncate", false)
    //      .start()
    //.awaitTermination()

    //    result.writeStream
    //      .format("console")
    //      .outputMode("update") // 不支持排序
    //      .option("truncate", false)
    //      .start()
    //      .awaitTermination()

    //        result.writeStream
    //          .format("console")
    //          .outputMode("append") //  不支持聚合
    //          .option("truncate", false)
    //          .start()
    //          .awaitTermination()


    val words = ds.flatMap(_.split("\\s+"))
    words.createOrReplaceTempView("word")
    val sql = spark.sql("select value, count(1) num from word group by value")

    val writer = new MysqlWriter("jdbc:mysql://node2:3306/test", "root", "123456")

    sql.writeStream
      //.format("console")
      .option("truncate", false)
      .outputMode("complete")
      .foreach(writer)
      .start()
      .awaitTermination()

    spark.stop()
  }

}


class MysqlWriter(url: String, user: String, password: String) extends ForeachWriter[Row] {

  var conn: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = DriverManager.getConnection(url, user, password)
    true
  }

  override def process(value: Row): Unit = {
    val sql = "replace into person(id, name, age) values(?, ?, ?)"
    val ps = conn.prepareStatement(sql)
    ps.setString(1, value(0).toString)
    ps.setString(2, value(1).toString)
    ps.setString(3, value(2).toString)
    ps.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}