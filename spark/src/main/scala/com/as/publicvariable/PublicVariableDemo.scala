package com.as.publicvariable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object PublicVariableDemo {

  /**
    * 1.广播变量 Broadcast variables
    * 2.累加器 Accumulators
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // 创建环境
    val conf: SparkConf = new SparkConf().setAppName("rddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // checkpoint
    sc.setCheckpointDir("./checkpoint") // 正式写入到hdfs上

    sc.setLogLevel("WARN")
    // 累加器
    val counter: LongAccumulator = sc.longAccumulator("counter")
    val rules = List(",",".",".","!")
    //  将变量广播到各个节点，如果直接使用list相当于driver把变量发送到各个task，有多少个task就需要发送多少次，
    //  广播变量是将变量发送到各个节点做缓存，task需要使用的时候直接从缓存中取就好了
    val broadcast: Broadcast[List[String]] = sc.broadcast(rules)
    sc.textFile("file/file1").filter(!_.isEmpty).flatMap(_.split(" "))
      .filter(word => {
        if(broadcast.value.contains(word)) {
          counter.add(1)
          false
        }else {
          true
        }
      })
  }

}
