package com.as.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SinkTrigger {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val data = spark.readStream.format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val result = data.as[String].coalesce(1).flatMap(_.split("\\s+"))
      .groupBy('value).count()//.orderBy('count.desc)

    result.writeStream.format("console").outputMode("complete")
      .trigger(Trigger.ProcessingTime("20 seconds")) //当时间为0时尽快处理
      //.trigger(Trigger.Continuous("1 second"))
      //.option("checkpointLocation", "./checkpoint") // Continuous processing does not support Sort operations
      .start()
      .awaitTermination()

    spark.stop()
  }

}
