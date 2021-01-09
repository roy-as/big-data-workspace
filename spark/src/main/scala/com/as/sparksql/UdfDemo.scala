package com.as.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object UdfDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val file: RDD[String] = sc.textFile("./file/text/file1")

    import spark.implicits._
    val data: DataFrame = file.flatMap(_.split("\\s+")).toDF()
    data.createOrReplaceTempView("file")

    spark.udf.register("uppercase", (value: String) => {
      value.toUpperCase()
    })

    spark.sql(
      """
        |select value, uppercase(value)
        |from file
      """.stripMargin).show()

    import org.apache.spark.sql.functions._
    val toupercase = udf((value:String) => {
      value.toUpperCase()
    })

    data.select('value, toupercase('value) as "uppercase").show()

    spark.stop()
  }

}
