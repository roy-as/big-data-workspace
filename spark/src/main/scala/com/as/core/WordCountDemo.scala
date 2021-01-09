package com.as.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val file: RDD[String] = sc.textFile("file/file2", 3)
    val data: RDD[(String, Int)] = file.filter(!_.isEmpty).flatMap(_.split("\\s+")).map(word => {
      println((word, 1))
      (word, 1)
    })
    val value: RDD[(String, Int)] = data.reduceByKey(_ + _)
    value.foreach(println)

  }
}
