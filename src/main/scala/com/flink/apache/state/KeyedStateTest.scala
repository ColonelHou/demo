package com.flink.apache.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 9999)
    stream
      .flatMap(_.trim.split("\\s+"))
      .map(x => {
        val arr = x.split(",")
        (arr(0).toDouble, arr(1))
      }).keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Double, String), (Double, String)] {

        val threshold = 2.5
        var temperatureState: ValueState[Double] = _
        override def open(parameters: Configuration): Unit = {
          // 声明一个键控状态
          temperatureState = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("temp", classOf[Double]))
        }

        override def flatMap(value: (Double, String), out: Collector[(Double, String)]): Unit = {
          val lastTemp = temperatureState.value()
          val nowTemp = value._1
          if (lastTemp > 0) {
            val diffTemp = (lastTemp - nowTemp).abs
            if (diffTemp > threshold) {
              val result = (value._1, s"两次温度超过$threshold")
              out.collect(result)
            }
          }
          temperatureState.update(nowTemp)
        }
      })
  }

}
