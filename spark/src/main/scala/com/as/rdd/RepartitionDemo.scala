package com.as.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RepartitionDemo {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val conf: SparkConf = new SparkConf().setAppName("rddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[Int] = sc.makeRDD(1 to 10, 3)

    /**3个分区
      * 7,8,9,10
      * 1,2,3
      * 4,5,6
      */
    rdd1.foreachPartition(iter => {
      iter.foreach(println(iter.hashCode(), _))
    })
    println("***********************************************")

    /**repartition再分区为4个分区
      * 1,6,10
      * 5,9
      * 3,4,8
      * 2,7
      *
      */
    rdd1.repartition(4).foreachPartition(iter => {
      iter.foreach(println(iter.hashCode(), _))
    })


    println("***********************************************")

    /**coalesce再分区为2个分区
      * 4,5,6,7,8,9,10
      * 1,2,3
      */
    rdd1.coalesce(2).foreachPartition(iter => {
      iter.foreach(println(iter.hashCode(), _))
    })

    println("***********************************************")

    /**coalesce再分区且shuffle为true
      * 2,4,6,8,10
      * 1,3,5,7,9
      */
    rdd1.coalesce(2, true).foreachPartition(iter => {
      iter.foreach(println(iter.hashCode(), _))
    })

    println("***********************************************")

    /**repartition为两个分区
      * 2,4,6,8,10
      * 1,3,5,7,9
      */
    rdd1.repartition(2).foreachPartition(iter => {
      iter.foreach(println(iter.hashCode(), _))
    })


  }

}
