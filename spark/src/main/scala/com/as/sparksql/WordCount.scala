package com.as.sparksql

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val rdd: RDD[String] = sc.textFile("./file/word")

    val words: RDD[String] = rdd.flatMap(_.split(".\\s+")).filter(StringUtils.isNotBlank(_))

    import spark.implicits._

    val df: DataFrame = words.toDF()

    //df.show()
    df.createOrReplaceTempView("word")

    df.show()

    spark.sql(
      """
        |select
        |value as word, count(1) num
        |from word
        |group by value
        |order by num desc
      """.stripMargin).show()

    df.groupBy('value)
      .count()
      .orderBy('count.desc)
      .show()


    spark.stop()
  }
}
