package com.as.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LocalWordCount {


  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setAppName("localWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 加载文件，处理数据
    val result: RDD[(String, Int)] = sc.textFile("/Users/aby/Desktop/file/file1")
                                      .flatMap(_.split(" "))
                                      .map((_, 1))
                                      .reduceByKey(_ + _)

    // sink,数据下沉
    result.foreach(println)

    println(result.collect().toBuffer)

    result.repartition(1).saveAsTextFile("result1")
    result.repartition(2).saveAsTextFile("result2")
    result.saveAsTextFile("result3")

    Thread.sleep(20 * 1000)

    sc.stop()


  }

}
