package com.as.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddDemo {


  def main(args: Array[String]): Unit = {

    // 创建环境
    val conf: SparkConf = new SparkConf().setAppName("rddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 加载资源
    val paraelize: RDD[Int] = sc.parallelize(1 to 10, 2)
    val makeRdd: RDD[Int] = sc.makeRDD(11 to 20)

    println(makeRdd.getNumPartitions)

    val file1: RDD[String] = sc.textFile("file/file1")
    val file2: RDD[String] = sc.textFile("file/file2", 4)
    val file: RDD[String] = sc.textFile("file/")
    val wholeTextFile: RDD[(String, String)] = sc.wholeTextFiles("file/")
    println(makeRdd.partitions.toBuffer.length)
    println(file1.getNumPartitions)
    println(file2.getNumPartitions)
    println(file.getNumPartitions)
    println(wholeTextFile.getNumPartitions)
    // map是针对分区中的每一条数据操作
    file1.flatMap(_.split(" ")).map(word => {
      println(word)
      (word, 1)
    }).collect().foreach(println)

    // mapPartitions是针对每个分区进行操作
    val result: RDD[(String, Int)] = file2.flatMap(_.split(" ")).mapPartitions(iter => {
      // 开启连接，有几个分区就进行几次
      iter.map((_, 1))
      // 关闭连接
    })
    result.coalesce(7, true)
    result.foreachPartition(iter => iter.foreach(println))


  }
}
