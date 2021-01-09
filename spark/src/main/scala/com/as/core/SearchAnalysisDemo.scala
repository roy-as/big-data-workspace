package com.as.core

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SearchAnalysisDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("searchDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 分词
    val wordAnalysis: SearchRecord => Array[(String, String)] = record => {
      val terms: util.List[Term] = HanLP.segment(record.keyWord)
      import scala.collection.JavaConverters._
      terms.asScala
        .map(_.word.replaceAll("\\[|\\]", ""))
        .filter(StringUtils.isNoneBlank(_))
        .map((record.userId, _))
        .toArray
    }

    val specialChar = Array("+", ".", "的")

    val files: RDD[String] = sc.textFile("file/SogouQ.sample")
    val records: RDD[SearchRecord] = files.filter(StringUtils.isNotBlank(_)).map(line => {
      val data: Array[String] = line.split("\\s+")
      SearchRecord(
        data(0), data(1), data(2), data(3).toInt, data(4).toInt, data(5)
      )
    })

    val words: RDD[(String, String)] = records.flatMap(record => wordAnalysis(record))


    words.map(_._2)
      .filter(!specialChar.contains(_))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)

    words.filter(word => !specialChar.contains(word._2))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)

    records.map(record => (record.time.substring(0, 5), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)

    sc.stop()

  }

}

case class SearchRecord(time: String, userId: String, keyWord: String, resultRank: Int, clickRank: Int, url: String)
