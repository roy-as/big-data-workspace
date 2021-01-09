package com.as.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowStream {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./check")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("119.3.169.76", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKeyAndWindow((num1:Int, num2:Int) => num1 + num2, Seconds(10),Seconds(5))
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
