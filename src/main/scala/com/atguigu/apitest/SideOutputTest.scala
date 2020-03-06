package com.atguigu.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/22 9:13
  */
object SideOutputTest {
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

    val highTempStream = dataStream
      .process(new SplitTempMonitor())

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[String]("low-temp")).print("low")

    env.execute("process function test")
  }
}

// 自定义process function，实现分流操作
class SplitTempMonitor() extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 判断当前数据的温度值，如果在30以下，输出到侧输出流
    if( value.temperature < 30 ){
      ctx.output(new OutputTag[String]("low-temp"), "warning")
    } else{
      // 30度以上的数据，输出到主流
      out.collect(value)
    }
  }
}