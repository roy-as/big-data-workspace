package com.as.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataFrameDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val csvFrame: DataFrame = spark.read.format(" ").csv("./file/csv")
    val textFrame: DataFrame = spark.read.format(" ").text("./file/text")
    val textFile: DataFrame = spark.read.json("./file/json")

    println(csvFrame.schema)
    csvFrame.show()

    println(textFile.schema)
    textFile.show()

    println(textFrame.schema)
    textFrame.show()

    spark.stop()


  }

}
