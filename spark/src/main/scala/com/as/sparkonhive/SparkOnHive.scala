package com.as.sparkonhive

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkOnHive {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://node2:9083")
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.sql("show databases").show(false)
    spark.sql("show tables").show(false)
    spark.stop()

  }

}
