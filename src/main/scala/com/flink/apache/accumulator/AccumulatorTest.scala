package com.flink.apache.accumulator

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.{IntCounter, LongCounter}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.registerCachedFile("", "")
//    IntCounter
    env.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")
        (arr(0).toInt, arr(1))
      }).map(new RichMapFunction[(Int, String), (Int, String)] {
      val total = new LongCounter()
      val normal = new LongCounter()
      val exception = new LongCounter()
      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("total", total)
        getRuntimeContext.addAccumulator("normal", normal)
        getRuntimeContext.addAccumulator("exception", exception)
      }
      override def map(value: (Int, String)): (Int, String) = {
        total.add(1)
        if (value._1 > 36 && value._1 < 37) {
          normal.add(1)
          (value._1, value._2 + " 正常")
        } else {
          exception.add(1)
          (value._1, value._2 + " 异常")
        }
      }
    })
    val result: JobExecutionResult = env.execute()
    // 把socket结束, 这里才会打印
    val total = result.getAccumulatorResult[Long]("total")
    val normal = result.getAccumulatorResult[Long]("normal")
    val exception = result.getAccumulatorResult[Long]("exception")
    println(total)
    println(normal)
    println(exception)

  }
}
