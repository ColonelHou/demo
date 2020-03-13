package com.flink.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/21 16:34
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    val streamFromFile: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val inputStream = env.socketTextStream("localhost", 7777)

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    val processedStream = dataStream
      .keyBy(_.id)
      .process(new TempIncreWarning())

    processedStream.print()

    env.execute("process function test")
  }
}

// 自定义实现process function
class TempIncreWarning() extends KeyedProcessFunction[String, SensorReading, String]{
  // 首先定义状态，用来保存上一次的温度值，以及已经设定的定时器时间戳
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val currentTimerState: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("current-timer", classOf[Long]) )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上次的温度值和定时器时间戳
    val lastTemp = lastTempState.value()
    val curTimerTs = currentTimerState.value()

    // 将状态更新为最新的温度值
    lastTempState.update(value.temperature)

    // 判断当前温度是否上升，如果上升而且没有注册定时器，那么注册10秒后的定时器
    if( value.temperature > lastTemp && curTimerTs == 0 ){
      val ts = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(ts)
      // 保存时间戳到定时器状态
      currentTimerState.update(ts)
    } else if( value.temperature < lastTemp ){
      // 如果温度下降，那么删除定时器和状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("sensor " + ctx.getCurrentKey + " 温度在10秒内连续上升")
    currentTimerState.clear()
  }
}