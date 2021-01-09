package com.as.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CaseObject {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val lines: Dataset[String] = spark.read.textFile("./file/persons")
    import spark.implicits._

    val persons: Dataset[Person] = lines.map(line => {
      val arr: Array[String] = line.split(";")
      Person(arr(0), arr(1).toInt, arr(2))
    })
    println(persons.schema)
    persons.show()

    val textFiles: RDD[String] = sc.textFile("./file/persons")
    val personRDD: RDD[Person] = textFiles.map(line => {
      val arr: Array[String] = line.split(";")
      Person(arr(0), arr(1).toInt, arr(2))
    })
    val personDataset: Dataset[Person] = personRDD.toDS()

    println(personDataset.schema)
    personDataset.show()

    val personDataFrame: DataFrame = personRDD.toDF()

    println(personDataFrame.schema)
    personDataFrame.show()


    val personSet: Dataset[Person] = personDataFrame.as[Person]
    personSet.show()

    val rdd1: RDD[Row] = personDataFrame.rdd

    val rdd2: RDD[Person] = personDataset.rdd



    val tupleRDD: RDD[(String, Int, String)] = textFiles.map(line => {
      val arr: Array[String] = line.split(";")
      (arr(0), arr(1).toInt, arr(2))
    })

    val tupleFrame: DataFrame = tupleRDD.toDF("name","age","habit")
    println("------------------tupleFrame------------------")
    tupleFrame.show()
    println("------------------tupleFrame------------------")


    spark.stop()

  }

}

case class Person(name:String, age:Int, habit: String)
