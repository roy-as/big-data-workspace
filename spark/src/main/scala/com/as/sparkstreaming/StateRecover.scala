package com.as.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StateRecover {

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getOrCreate("./check", getContext)
    ssc.sparkContext.setLogLevel("WARN")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  def getContext(): StreamingContext = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("119.3.169.76", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1)).updateStateByKey((current: Seq[Int], history: Option[Int]) => {
      if (null != current && current.nonEmpty) {
        Some(current.sum + history.getOrElse(0))
      } else {
        history
      }
    })
    result.print()

    ssc
  }

}
