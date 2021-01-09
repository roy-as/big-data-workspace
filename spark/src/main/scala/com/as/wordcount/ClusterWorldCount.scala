package com.as.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ClusterWorldCount {

  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      println("参数错误")
      System.exit(1)
    }
    val conf: SparkConf = new SparkConf().setAppName("clusterWordCount")
    val sc = new SparkContext(conf)

    val result: RDD[(String, Int)] = sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.foreach(println)
    result.saveAsTextFile(args(1))

    sc.stop()

  }


}
