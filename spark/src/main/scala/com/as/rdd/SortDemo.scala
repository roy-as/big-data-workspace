package com.as.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortDemo {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val conf: SparkConf = new SparkConf().setAppName("rddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val result: RDD[(String, Int)] = sc.textFile("file/file1").filter(!_.isEmpty).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    val resultSortBy: Array[(String, Int)] = result.sortBy(_._2, false).take(3)
    println(resultSortBy.toBuffer)

    val sortResult: Array[(Int, String)] = result.map(_.swap).sortByKey(false).take(3)
    println(sortResult.toBuffer)

    val topResult: Array[(String, Int)] = result.top(3)(Ordering.by(_._2))
    println(topResult.toBuffer)
  }

}
