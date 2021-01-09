package com.as.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TopNStreaming {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("./check")

    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("119.3.169.76", 9999)

    val result: DStream[(String, Int)] = dStream.flatMap(_.split("\\s+")).map((_, 1))
      .reduceByKeyAndWindow((num1: Int, num2: Int) => num1 + num2, Seconds(40), Seconds(20))
      .transform(rdd => {
        println(rdd)
        println("********************top*************************")
        rdd.sortBy(_._2, false).take(3).foreach(println)
        println("********************top*************************")
        rdd
      })


    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
