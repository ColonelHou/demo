package com.flink.apache.real2019

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * 338,123
 * 39,123
 * 36,123
 * 39,00
 */
object Real2019 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.registerCachedFile("", "")
//    IntCounter
    env.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")
        if (arr(0).toInt > 37) {
          ("体温异常", 1)
        } else {
          ("体温正常", 1)
        }
      }).keyBy(_._1).sum(1).print()
    env.execute()

  }
}
