package com.flink.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/19 13:56
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val streamFromFile: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // Transform操作
    val dataStream: DataStream[SensorReading] = streamFromFile
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      } )

    // 1. 滚动聚合
    val sumStream: DataStream[SensorReading] = dataStream
      .keyBy(0)
      .sum(2)    // 对不同id的温度值做累加
    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .reduce( (reduceRes, curData) => SensorReading( reduceRes.id, reduceRes.timestamp + 1, curData.temperature + 10 ) ) // 对不同id的温度值，输出当前的温度+10，并将时间戳在聚合结果上+1

    // 2. 分流操作
    val splitStream: SplitStream[SensorReading] = dataStream
      .split( sensorData => {
        if( sensorData.temperature > 30 )
          Seq("high")
        else
          Seq("low")
      } )
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    // 3. 合流操作
    val warningStream: DataStream[(String, Double, String)] = highTempStream
      .map( data => (data.id, data.temperature, "high temperature warning") )

    val connectedStreams: ConnectedStreams[(String, Double, String), SensorReading] = warningStream
      .connect(lowTempStream)

    val coMapStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2),
      lowData => (lowData.id, lowData.temperature, "healthy")
    )

    val unionStream = highTempStream.union(lowTempStream)

//    reduceStream.print()
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")

    // 4. 函数类示例
    dataStream.filter( new MyFilter() ).print()

//    coMapStream.print()
    env.execute("transform test")
  }
}

// 自定义Filter Function
class MyFilter() extends RichFilterFunction[SensorReading]{

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.getIndexOfThisSubtask
  }

  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}