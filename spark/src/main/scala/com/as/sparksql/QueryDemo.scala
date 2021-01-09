package com.as.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

object QueryDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    val lines: Dataset[String] = spark.read.textFile("./file/persons")

    val persons: Dataset[Person] = lines.map(line => {
      val arr: Array[String] = line.split(";")
      Person(arr(0), arr(1).toInt, arr(2))
    })
    persons.createOrReplaceTempView("person")

    spark.sql(
      """
        |select *
        |from person
        |where age > 21
      """.stripMargin).show()

    println(persons.where('age > 21).count())

    persons.groupBy('age).count().show()
    import org.apache.spark.sql.functions._
    persons.agg(
      avg('age) as "avg_age"
    ).show()
    spark.stop()
  }

}