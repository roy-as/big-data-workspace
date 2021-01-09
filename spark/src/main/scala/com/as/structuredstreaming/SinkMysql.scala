package com.as.structuredstreaming

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object SinkMysql {

  def main(args: Array[String]): Unit = {
    //TODO 0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val data = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()
    val ds = data.as[String]
    val result = ds.flatMap(_.split("\\s+"))
      .groupBy('value)
      .count()
      .orderBy('count.desc)

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    result.writeStream
        .format("jdbc")
      .outputMode("complete")
      .foreachBatch((data, batchId) => {
        println("-----------------")
        println(s"batchID:$batchId")
        println("-----------------")
        data.show()
        data.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
//          .option("url", "jdbc:mysql://node2:3306/test?characterEncoding=UTF-8")
//          .option("user", "root")
//          .option("password", "123456")
//          .option("dbtable", "word")
          //.save
          .jdbc("jdbc:mysql://node2:3306/test", "word", prop)

      })
      .start()
      .awaitTermination()

    spark.stop()

  }

}
