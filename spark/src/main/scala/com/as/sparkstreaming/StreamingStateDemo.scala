package com.as.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingStateDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./check")
    val updateFunc: (Seq[Int], Option[Int]) => Option[Int] = (current, history) => {
      if (null != current && current.nonEmpty) {
        val result: Int = current.sum + history.getOrElse(0)
        Some(result)
      } else {
        history
      }
    }

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("119.3.169.76", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1)).updateStateByKey(updateFunc)
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }
}
