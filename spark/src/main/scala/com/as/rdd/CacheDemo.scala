package com.as.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val conf: SparkConf = new SparkConf().setAppName("rddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // checkpoint
    sc.setCheckpointDir("./checkpoint") // 正式写入到hdfs上

    sc.setLogLevel("WARN")

    val result: RDD[(String, Int)] = sc.textFile("file/file1").flatMap(word => {
      println("******", word, "********")
      word.split(" ")
    }).map((_, 1))
    // persist
    result.persist()
    // checkpoint
    result.checkpoint()
    println("************")
    result.reduceByKey(_ + _).foreach(println)
    println("*************")
    result.sortByKey().top(3)(Ordering.by(_._2)).foreach(println)
  }

}
