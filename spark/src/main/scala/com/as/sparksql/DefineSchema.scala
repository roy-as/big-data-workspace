package com.as.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DefineSchema {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("./file/persons")
    val rows: RDD[Row] = lines.map(line => {
      val arr: Array[String] = line.split(";")
      Row(arr(0), arr(1).toInt, arr(2))
    })

    val structType = StructType(Array(
      StructField("name", StringType, false),
      StructField("age", IntegerType, false),
      StructField("habit", StringType, false)
    ))
    val dataFrame: DataFrame = spark.createDataFrame(rows, structType)
    println(dataFrame.schema)
    dataFrame.show()

    spark.stop()
  }

}
